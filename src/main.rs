//! blobstore-s3 capability provider
//!
//!
//#[allow(unused_imports)]
use blobstore_interface::{
    BlobList, BlobReceiver, BlobReceiverSender, Blobstore, BlobstoreReceiver, BlobstoreResult,
    Container, FileBlob, FileChunk, GetObjectInfoRequest, RemoveObjectRequest,
    StartDownloadRequest,
};
use chrono::{DateTime, FixedOffset, Utc};
use futures::TryStreamExt;
use hyper::{Client, Uri};
use hyper_proxy::{Intercept, Proxy, ProxyConnector};
use hyper_tls::HttpsConnector;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use rusoto_core::credential::StaticProvider;
use rusoto_core::Region;
use rusoto_s3::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    HeadObjectOutput, HeadObjectRequest, ListObjectsV2Output, ListObjectsV2Request,
    PutObjectRequest, S3Client, S3,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, RwLock},
};
use wasmbus_rpc::provider::prelude::*;

type HttpConnector =
    hyper_proxy::ProxyConnector<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>;

const DEFAULT_AWS_ACCESS_KEY: &str = "minio";
const DEFAULT_AWS_SECRET_KEY: &str = "minio123";
const ENV_AWS_ACCESS_KEY: &str = "AWS_ACCESS_KEY";
const ENV_AWS_ACCESS_SECRET: &str = "AWS_SECRET_ACCESS_KEY";
const ENV_AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";
const ENV_AWS_REGION_ID: &str = "REGION";
const ENV_TOKEN_EXPIRATION_KEY: &str = "AWS_CREDENTIAL_EXPIRATION";
//const ENV_MINIO_ADDRESS: &str = "0.0.0.0";

// main (via provider_main) initializes the threaded tokio executor,
// listens to lattice rpcs, handles actor links,
// and returns only when it receives a shutdown message
//
fn main() -> Result<(), Box<dyn std::error::Error>> {
    provider_main(BlobstoreS3Provider::default())?;

    eprintln!("blobstore-s3 provider exiting");
    Ok(())
}

/// Configuration for connecting to an S3 server
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct S3ClientConfiguration {
    #[serde(default)]
    region: Region,
    #[serde(default)]
    access_key_id: String,
    #[serde(default)]
    access_key_secret: String,
    #[serde(default)]
    session_token: Option<String>,
    #[serde(default)]
    expiration: Option<DateTime<Utc>>,
}

impl S3ClientConfiguration {
    fn new_from(values: &HashMap<String, String>) -> RpcResult<S3ClientConfiguration> {
        let mut config = if let Some(config_b64) = values.get("config_b64") {
            let bytes = base64::decode(config_b64.as_bytes()).map_err(|e| {
                RpcError::InvalidParameter(format!("invalid base64 encoding: {}", e))
            })?;
            serde_json::from_slice::<S3ClientConfiguration>(&bytes)
                .map_err(|e| RpcError::InvalidParameter(format!("corrupt config_b64: {}", e)))?
        } else if let Some(config) = values.get("config_json") {
            serde_json::from_str::<S3ClientConfiguration>(config)
                .map_err(|e| RpcError::InvalidParameter(format!("corrupt config_json: {}", e)))?
        } else {
            S3ClientConfiguration::default()
        };
        if let Some(access_key) = values.get(ENV_AWS_ACCESS_KEY) {
            config.access_key_id = access_key.clone();
        } else {
            config.access_key_id = String::from(DEFAULT_AWS_ACCESS_KEY);
        }
        if let Some(access_secret) = values.get(ENV_AWS_ACCESS_SECRET) {
            config.access_key_secret = access_secret.clone();
        } else {
            config.access_key_secret = String::from(DEFAULT_AWS_SECRET_KEY);
        }
        if let Some(session_token) = values.get(ENV_AWS_SESSION_TOKEN) {
            config.session_token = Some(session_token.clone());
        }
        config.region = if values.contains_key(ENV_AWS_REGION_ID) {
            Region::Custom {
                name: values[ENV_AWS_REGION_ID].clone(),
                endpoint: if values.contains_key("ENDPOINT") {
                    values["ENDPOINT"].clone()
                } else {
                    "http://127.0.0.1:9000".to_string()
                    //"s3.us-east-1.amazonaws.com".to_string()
                },
            }
        } else {
            Region::UsEast1
        };
        config.expiration = match values.get(ENV_TOKEN_EXPIRATION_KEY) {
            Some(val) => Some(
                DateTime::<FixedOffset>::parse_from_rfc3339(&val)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|e| {
                        RpcError::InvalidParameter(format!(
                            "invalid value for token expiration in environment: {}",
                            e
                        ))
                    })?,
            ),
            _ => None,
        };

        Ok(config)
    }
}

#[derive(Debug, PartialEq)]
struct FileUpload {
    container: String,
    id: String,
    total_bytes: u64,
    expected_chunks: u64,
    chunks: Vec<FileChunk>,
}

impl FileUpload {
    fn is_complete(&self) -> bool {
        self.chunks.len() == self.expected_chunks as usize
    }
}

/// blobstore-s3 capability provider implementation
#[derive(Default, Clone)]
struct BlobstoreS3Provider {
    clients: Arc<RwLock<HashMap<String, S3Client>>>,
    uploads: Arc<RwLock<HashMap<String, FileUpload>>>,
    links: Arc<RwLock<HashMap<String, LinkDefinition>>>,
}

fn client_for_config(config_map: &HashMap<String, String>) -> RpcResult<S3Client> {
    let region = if config_map.contains_key("REGION") {
        Region::Custom {
            name: config_map["REGION"].clone(),
            endpoint: if config_map.contains_key("ENDPOINT") {
                config_map["ENDPOINT"].clone()
            } else {
                "http://127.0.0.1:9000".to_string()
            },
        }
    } else {
        Region::Custom {
            name: "us-east-1".to_string(),
            endpoint: if config_map.contains_key("ENDPOINT") {
                config_map["ENDPOINT"].clone()
            } else {
                "http://127.0.0.1:9000".to_string()
            },
        }
    };

    let client = if config_map.contains_key("AWS_ACCESS_KEY") {
        let provider = StaticProvider::new(
            config_map["AWS_ACCESS_KEY"].to_string(),
            config_map["AWS_SECRET_ACCESS_KEY"].to_string(),
            config_map.get("AWS_TOKEN").cloned(),
            config_map
                .get("TOKEN_VALID_FOR")
                .map(|t| t.parse::<i64>().unwrap()),
        );
        let connector: HttpConnector = if let Some(proxy) = config_map.get("HTTP_PROXY") {
            let proxy = Proxy::new(Intercept::All, proxy.parse::<Uri>().unwrap());
            ProxyConnector::from_proxy(hyper_tls::HttpsConnector::new(), proxy).unwrap()
        } else {
            ProxyConnector::new(HttpsConnector::new()).unwrap()
        };
        let mut hyper_builder: hyper::client::Builder = Client::builder();
        hyper_builder.pool_max_idle_per_host(0);
        let client = rusoto_core::HttpClient::from_builder(hyper_builder, connector);
        S3Client::new_with(client, provider, region)
    } else {
        let provider = StaticProvider::new(
            DEFAULT_AWS_ACCESS_KEY.to_string(),
            DEFAULT_AWS_SECRET_KEY.to_string(),
            config_map.get("AWS_TOKEN").cloned(),
            config_map
                .get("TOKEN_VALID_FOR")
                .map(|t| t.parse::<i64>().unwrap()),
        );
        S3Client::new_with(
            rusoto_core::request::HttpClient::new()
                .expect("Failed to create HTTP client for S3 provider"),
            provider,
            region,
        )
    };

    Ok(client)
}

/// use default implementations of provider message handlers
impl ProviderDispatch for BlobstoreS3Provider {}
impl BlobstoreReceiver for BlobstoreS3Provider {}
#[async_trait]
impl ProviderHandler for BlobstoreS3Provider {
    async fn put_link(&self, ld: &LinkDefinition) -> RpcResult<bool> {
        // self.clients.write().unwrap().insert(config.module.clone(), Arc::new(s3::client_for_config(&config)?),);
        // TODO: incorporate S3ClientConfiguration
        let _config = S3ClientConfiguration::new_from(&ld.values)?;
        // TODO: write client_for_config!!!
        self.clients
            .write()
            .unwrap()
            .insert(ld.actor_id.to_string(), client_for_config(&ld.values)?);
        self.links
            .write()
            .unwrap()
            .insert(ld.actor_id.to_string(), ld.clone());
        Ok(true)
    }

    async fn delete_link(&self, actor_id: &str) {
        // self.clients.write().unwrap().remove(_actor_id)
        self.clients.write().unwrap().remove(actor_id);
    }

    async fn shutdown(&self) -> Result<(), Infallible> {
        Ok(())
    }
}

impl BlobstoreS3Provider {
    async fn manage_chunk_dispatch(
        &self,
        idx: u64,
        client: S3Client,
        container: String,
        id: String,
        chunk_size: u64,
        byte_size: u64,
        actor_id: String,
    ) -> RpcResult<()> {
        let start = idx * chunk_size;
        let mut end = start + chunk_size - 1;
        if end > byte_size {
            end = byte_size - 1;
        }

        let bytes = get_blob_range(&client, &container, &id, start, end)
            .await
            .unwrap();
        let fc = FileChunk {
            chunk_bytes: bytes,
            chunk_size: chunk_size,
            container: Container {
                id: container.clone(),
            },
            context: None,
            id: id.clone(),
            sequence_no: idx + 1,
            total_bytes: byte_size,
        };

        let rd = self.links.read().unwrap().clone();
        let link_def = rd
            .get(&actor_id)
            .ok_or_else(|| RpcError::Other(format!("Could not retrieve link for {}", actor_id)))?;
        let this = self.clone();
        let cloned_link_def = link_def.clone();
        tokio::spawn(async move { this.dispatch_chunk(&cloned_link_def, fc).await });
        //tokio::spawn(async move { this.dispatch_chunk(&cloned_link_def, &fc).await });

        Ok(())
    }

    //async fn dispatch_chunk(&self, ld: &LinkDefinition, chunk: &FileChunk) {
    async fn dispatch_chunk(&self, ld: &LinkDefinition, chunk: FileChunk) {
        let ld_clone = ld.clone();
        let chunk_clone = chunk.clone();
        let handle = tokio::runtime::Handle::current();
        async {
            if let Err(e) = handle
                .spawn(async move {
                    let actor = BlobReceiverSender::for_actor(&ld_clone);
                    if let Err(e) = actor.receive_chunk(&Context::default(), &chunk_clone).await {
                        error!(
                            "chunk send to actor: {}, err: {}",
                            &ld_clone.actor_id,
                            e.to_string()
                        );
                    };
                })
                .await
            {
                error!(
                    "dispatch_chunk(): error in handle.spawn(): {}",
                    e.to_string()
                );
            }
        }
        .await;
    }
}

/// Handle Blobstore methods
#[async_trait]
impl Blobstore for BlobstoreS3Provider {
    /// creates a new container
    async fn create_container<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<Container> {
        let container_id = arg.to_string();
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let rd = self.clients.read().unwrap();
        let s3_client = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(create_bucket(container_id.clone(), s3_client))?;
        Ok(Container {
            id: container_id.clone(),
        })
    }

    /// RemoveContainer(id: string): BlobstoreResult
    async fn remove_container<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<BlobstoreResult> {
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let bucket_id = arg.to_string();
        let rd = self.clients.read().unwrap();
        let s3_client = rd
            .get(actor_id)
            .ok_or_else(|| RpcError::InvalidParameter(format!("actor not linked:{}", actor_id)))?;
        let rt = tokio::runtime::Runtime::new().unwrap();
        Ok(rt
            .block_on(remove_bucket(bucket_id, s3_client))
            .map_or_else(
                |e| BlobstoreResult {
                    success: false,
                    error: Some(e.to_string()),
                },
                |_| BlobstoreResult {
                    success: true,
                    error: None,
                },
            ))
    }

    /// remove_object()
    async fn remove_object(
        &self,
        ctx: &Context,
        arg: &RemoveObjectRequest,
    ) -> RpcResult<BlobstoreResult> {
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let container_id = arg.container_id.to_string();
        let object_id = arg.id.to_string();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(remove_object(
            container_id,
            object_id,
            &self.clients.read().unwrap()[actor_id],
        ))
        .map_or_else(
            |e| Err(RpcError::from(format!("remove_object(): {}", e))),
            |_| {
                Ok(BlobstoreResult {
                    success: true,
                    error: None,
                })
            },
        )
    }

    /// list_objects(container_id: string): BlobList
    async fn list_objects<TS: ToString + ?Sized + std::marker::Sync>(
        &self,
        ctx: &Context,
        arg: &TS,
    ) -> RpcResult<BlobList> {
        let container_id = arg.to_string();
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        /*
        async fn list_objects(
            container_id: String,
            client: &S3Client,
        ) -> Result<Option<Vec<Object>>, Box<dyn std::error::Error + Sync + Send>> {
            let list_obj_req = ListObjectsV2Request {
                bucket: container_id.to_owned(),
                ..Default::default()
            };
            let res: ListObjectsV2Output = client.list_objects_v2(list_obj_req).await?;
            Ok(res.contents)
        }
                 */
        let client = &self.clients.read().unwrap()[actor_id].clone();
        //let objects = list_objects(container_id, client).await.unwrap();
        let list_obj_req = ListObjectsV2Request {
            bucket: container_id.trim_matches('/').to_owned(),
            ..Default::default()
        };
        let res: ListObjectsV2Output = client
            .list_objects_v2(list_obj_req)
            .await
            .map_err(|e| RpcError::Other(format!("list_objects_v2_error(): {}", e).to_string()))?;
        let objects = res.contents;
        /*
        let result = head_object(
            &s3_client,
            arg.container_id.to_string(),
            arg.blob_id.to_string(),
        )
        .await
        .map_or_else(
            |_| FileBlob {
                id: "none".to_string(),
                container: Container {
                    id: "none".to_string(),
                },
                byte_size: 0,
            },
            |ob| FileBlob {
                id: arg.blob_id.to_string(),
                container: Container {
                    id: arg.container_id.to_string(),
                },
                byte_size: ob.content_length.unwrap() as u64,
            },
        );
        */
        let blobs = if let Some(v) = objects {
            v.iter()
                .map(|ob| FileBlob {
                    id: ob.key.clone().unwrap(),
                    container: Container {
                        id: arg.to_string().trim_matches('/').to_string(),
                    },
                    byte_size: ob.size.unwrap() as u64,
                })
                .collect()
        } else {
            BlobList::new()
        };
        Ok(blobs)
    }

    /// upload_chunk(chunk: FileChunk): BlobstoreResult
    async fn upload_chunk(&self, ctx: &Context, arg: &FileChunk) -> RpcResult<BlobstoreResult> {
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let key = upload_key(&arg.container.id, &arg.id, actor_id);
        self.uploads
            .write()
            .unwrap()
            .entry(key.clone())
            .and_modify(|u| {
                u.chunks.push(arg.clone());
            });
        if self.uploads.read().unwrap()[&key].is_complete() {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let upload_res = rt
                .block_on(complete_upload(
                    &self.clients.read().unwrap()[actor_id],
                    &self.uploads.read().unwrap()[&key],
                ))
                .map_err(|e| RpcError::from(format!("complete_upload(): {}", e)));
            self.uploads.write().unwrap().remove(&key);
            return upload_res;
        }
        Ok(build_empty_blobstore_result())
    }

    /// start_download(blob_id: string, container_id: string, chunk_size: u64, context: string?): BlobstoreResult
    async fn start_download(
        &self,
        ctx: &Context,
        arg: &StartDownloadRequest,
    ) -> RpcResult<BlobstoreResult> {
        // TODO: call BlobReceiver's receive_chunk() to pass chunk to actor
        // NOTE receive_chunk(chunk: FileChunk) is an *actor* interface that receives chunks
        //      sent by Blobstore provider
        // NOTE see the interface 'MessageSubscriber' in messaging.smithy and the NATS messaging implementation of this interface
        //      XXX (the implemented function is called 'handle_message()')
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?
            .clone();
        let s3_client = self.clients.read().unwrap()[&actor_id.to_string()].clone();
        //let container_id = String::from("whatever");
        let initial_container_id = arg.container_id.clone();
        let container_id = initial_container_id.trim_matches('/').to_string();
        //let container_id = arg.container_id.trim_matches('/').clone();
        //let chunk_size = 0;
        let chunk_size = arg.chunk_size;
        //let blob_id = String::from("fake_blob_id");
        let initial_blob_id = arg.blob_id.clone();
        let blob_id = initial_blob_id
            .trim_matches(|c| c == '/' || c == '?')
            .to_string();
        //let blob_id = arg.blob_id.trim_matches(|c| c == '/' || c == '?').clone();
        let this = self.clone();

        let byte_size = {
            let info = head_object(&s3_client, container_id.clone(), blob_id.clone())
                .await
                .unwrap();
            info.content_length.unwrap() as u64
        };

        let actor_id = actor_id.to_string();

        let chunk_count = expected_chunks(byte_size, chunk_size);
        let handle = tokio::runtime::Handle::current();
        //futures::executor::block_on(async {
        async {
            for idx in 0..chunk_count {
                // START FOR LOOP
                let actor_id_clone = actor_id.clone();
                let s3_client_clone = s3_client.clone();
                let container_id_clone = container_id.clone();
                let blob_id_clone = blob_id.clone();
                let this_clone = this.clone();
                handle
                    .spawn(async move {
                        this_clone
                            .manage_chunk_dispatch(
                                idx,
                                s3_client_clone,
                                container_id_clone,
                                blob_id_clone,
                                chunk_size,
                                byte_size,
                                actor_id_clone,
                            )
                            .await
                            .map_err(|e| RpcError::from(format!("start_download(): {}", e)))
                            .unwrap();
                    })
                    .await
                    .map_err(|e| RpcError::from(format!("start_download() handle spawn(): {}", e)))
                    .unwrap(); // end handle.spawn()
            } // END FOR LOOP
            Ok(BlobstoreResult {
                success: true,
                error: None,
            })
        }
        .await
        //});
        /*
        Ok(BlobstoreResult {
            success: true,
            error: None,
        })
        */
    }

    /// start_upload(self, ctx, arg: &FileChunk) -> RpcResult<BlobstoreResult>
    async fn start_upload(&self, ctx: &Context, arg: &FileChunk) -> RpcResult<BlobstoreResult> {
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?;
        let key = upload_key(&arg.container.id, &arg.id, actor_id);

        let upload = FileUpload {
            chunks: vec![],
            container: arg.container.id.to_string(),
            id: arg.id.to_string(),
            total_bytes: arg.total_bytes,
            expected_chunks: expected_chunks(arg.total_bytes, arg.chunk_size),
        };
        self.uploads.write().unwrap().insert(key, upload);
        Ok(BlobstoreResult {
            success: true,
            error: None,
        })
    }

    /// get_object_info(self, ctx, GetObjectInfoRequest) -> RpcResult<FileBlob>
    async fn get_object_info(
        &self,
        ctx: &Context,
        arg: &GetObjectInfoRequest,
    ) -> RpcResult<FileBlob> {
        let actor_id = ctx
            .actor
            .as_ref()
            .ok_or_else(|| RpcError::InvalidParameter("no actor in request".to_string()))?
            .clone();
        let s3_client = self.clients.read().unwrap()[&actor_id.to_string()].clone();
        let result = head_object(
            &s3_client,
            arg.container_id.trim_matches('/').to_string(),
            arg.blob_id
                .trim_matches(|c| c == '/' || c == '?')
                .to_string(),
        )
        .await
        .map_or_else(
            |_| FileBlob {
                id: "none".to_string(),
                container: Container {
                    id: "none".to_string(),
                },
                byte_size: 0,
            },
            |ob| FileBlob {
                id: arg
                    .blob_id
                    .trim_matches(|c| c == '/' || c == '?')
                    .to_string(),
                container: Container {
                    id: arg.container_id.trim_matches('/').to_string(),
                },
                byte_size: ob.content_length.unwrap() as u64,
            },
        );
        Ok(result)
    }
}

async fn get_blob_range(
    client: &S3Client,
    container_id: &str,
    blob_id: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Sync + Send>> {
    let get_req = GetObjectRequest {
        bucket: container_id.to_owned(),
        key: blob_id.to_owned(),
        range: Some(format!("bytes={}-{}", start, end)),
        ..Default::default()
    };

    let result = client.get_object(get_req).await?;
    let stream = result.body.unwrap();
    let body = stream
        .map_ok(|b| bytes::BytesMut::from(&b[..]))
        .try_concat()
        .await
        .unwrap();
    Ok(body.to_vec())
}

fn expected_chunks(total_bytes: u64, chunk_size: u64) -> u64 {
    let mut chunks = total_bytes / chunk_size;
    if total_bytes % chunk_size != 0 {
        chunks = chunks + 1
    }
    chunks
}

async fn create_bucket(container_id: String, client: &S3Client) -> RpcResult<Container> {
    let create_bucket_req = CreateBucketRequest {
        bucket: container_id.trim_matches('/').to_string(),
        ..Default::default()
    };
    client
        .create_bucket(create_bucket_req)
        .await
        .map_err(|_e| RpcError::Other("create_bucket() failed".to_string()))
        .unwrap();
    Ok(Container {
        id: container_id.trim_matches('/').to_string(),
    })
}

async fn remove_bucket(
    container_id: String,
    client: &S3Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let delete_bucket_req = DeleteBucketRequest {
        bucket: container_id.to_owned(),
        ..Default::default()
    };

    client.delete_bucket(delete_bucket_req).await?;
    Ok(())
}

async fn complete_upload(
    client: &S3Client,
    upload: &FileUpload,
) -> Result<BlobstoreResult, Box<dyn std::error::Error + Sync + Send>> {
    let bytes = upload
        .chunks
        .iter()
        .fold(vec![], |a, c| [&a[..], &c.chunk_bytes[..]].concat());
    let put_request = PutObjectRequest {
        bucket: upload.container.to_string(),
        key: upload.id.to_string(),
        body: Some(bytes.into()),
        ..Default::default()
    };

    client.put_object(put_request).await?;
    Ok(BlobstoreResult {
        error: None,
        success: true,
    })
}

async fn remove_object(
    container_id: String,
    object_id: String,
    client: &S3Client,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    let delete_object_req = DeleteObjectRequest {
        bucket: container_id.to_string(),
        key: object_id.to_string(),
        ..Default::default()
    };

    client.delete_object(delete_object_req).await?;
    Ok(())
}

/*
async fn list_objects(
    container_id: String,
    client: &S3Client,
) -> Result<Option<Vec<Object>>, Box<dyn std::error::Error + Sync + Send>> {
    let list_obj_req = ListObjectsV2Request {
        bucket: container_id.to_owned(),
        ..Default::default()
    };
    let res: ListObjectsV2Output = client.list_objects_v2(list_obj_req).await?;
    Ok(res.contents)
}
*/

async fn head_object(
    client: &S3Client,
    container_id: String,
    key: String,
) -> Result<HeadObjectOutput, Box<dyn std::error::Error + Sync + Send>> {
    let head_req = HeadObjectRequest {
        bucket: container_id.trim_matches('/').to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };

    client.head_object(head_req).await.map_err(|e| e.into())
}

fn upload_key(container_id: &str, blob_id: &str, actor_id: &str) -> String {
    format!("{}-{}-{}", actor_id, container_id, blob_id)
}

// helper function which should only be used for function stubs
fn build_empty_blobstore_result() -> BlobstoreResult {
    return BlobstoreResult {
        error: None,
        success: false,
    };
}

/// Handle incoming rpc messages and dispatch to applicable trait handler.
#[async_trait]
impl MessageDispatch for BlobstoreS3Provider {
    async fn dispatch(&self, ctx: &Context, message: Message<'_>) -> RpcResult<Message<'_>> {
        let op = match message.method.split_once('.') {
            Some((cls, op)) if cls == "Blobstore" => op,
            None => message.method,
            _ => {
                return Err(RpcError::MethodNotHandled(message.method.to_string()));
            }
        };
        BlobstoreReceiver::dispatch(
            self,
            ctx,
            &Message {
                method: op,
                arg: message.arg,
            },
        )
        .await
    }
}
