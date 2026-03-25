use bytes::Bytes;
use icechunk::{
    Repository, Storage, Store,
    format::{
        Path,
        format_constants::SpecVersionBin,
        manifest::{VirtualChunkLocation, VirtualChunkRef},
    },
    session::VirtualObjectReference,
    storage::ObjectStorage,
};
use icechunk_macros::tokio_test;
use rstest::rstest;
use rstest_reuse::{self, *};
use std::{error::Error, sync::Arc};
use tokio::sync::RwLock;

#[template]
#[rstest]
#[case::v1(SpecVersionBin::V1)]
#[case::v2(SpecVersionBin::V2)]
fn spec_version_cases(#[case] spec_version: SpecVersionBin) {}

async fn create_repo(
    spec_version: SpecVersionBin,
) -> (Repository, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
        ObjectStorage::new_local_filesystem(tmp.path())
            .await
            .unwrap(),
    );
    let repo = Repository::create(
        None,
        storage,
        Default::default(),
        Some(spec_version),
        true,
    )
    .await
    .unwrap();
    (repo, tmp)
}

/// Helper to set up a store with a 2D array (shape 20x20, chunks 10x10
/// → 2x2 chunk grid) and virtual refs on some chunks.
async fn setup_store_with_virtual_chunks(
    spec_version: SpecVersionBin,
) -> (Store, tempfile::TempDir) {
    let (repo, tmp) = create_repo(spec_version).await;
    let ds = repo.writable_session("main").await.unwrap();
    let store =
        Store::from_session(Arc::new(RwLock::new(ds))).await;

    // Set up zarr v3 group
    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group"}"#,
            ),
        )
        .await
        .unwrap();

    // Set up 2D array: shape [20, 20], chunks [10, 10] → 2x2 chunk grid
    let zarr_meta = Bytes::copy_from_slice(
        br#"{"zarr_format":3,"node_type":"array","shape":[20,20],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[10,10]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#,
    );
    store.set("array/zarr.json", zarr_meta).await.unwrap();

    // Set virtual refs for chunks (0,0), (0,1), (1,0)
    let vref_0_0 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "s3://bucket/file0.nc",
        )
        .unwrap(),
        offset: 0,
        length: 1000,
        checksum: None,
    };
    let vref_0_1 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "s3://bucket/file1.nc",
        )
        .unwrap(),
        offset: 0,
        length: 2000,
        checksum: None,
    };
    let vref_1_0 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "s3://bucket/file2.nc",
        )
        .unwrap(),
        offset: 100,
        length: 500,
        checksum: None,
    };

    store
        .set_virtual_ref("array/c/0/0", vref_0_0, false)
        .await
        .unwrap();
    store
        .set_virtual_ref("array/c/0/1", vref_0_1, false)
        .await
        .unwrap();
    store
        .set_virtual_ref("array/c/1/0", vref_1_0, false)
        .await
        .unwrap();

    // Set a materialized chunk at (1,1)
    store
        .set("array/c/1/1", Bytes::from(vec![0u8; 400]))
        .await
        .unwrap();

    (store, tmp)
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_get_virtual_chunk_references_full_bbox(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let (store, _tmp) =
        setup_store_with_virtual_chunks(spec_version).await;

    let session = store.session();
    let session = session.read().await;

    // Full bbox: all 4 chunks (0..1, 0..1)
    let refs = session
        .get_virtual_chunk_references(
            &Path::new("/array").unwrap(),
            &[(0, 1), (0, 1)],
        )
        .await?;

    // Should return 3 virtual refs (not the materialized one at (1,1))
    assert_eq!(refs.len(), 3);

    let locations: Vec<&str> =
        refs.iter().map(|r| r.location.as_str()).collect();
    assert!(locations.contains(&"s3://bucket/file0.nc"));
    assert!(locations.contains(&"s3://bucket/file1.nc"));
    assert!(locations.contains(&"s3://bucket/file2.nc"));

    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_get_virtual_chunk_references_single_chunk(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let (store, _tmp) =
        setup_store_with_virtual_chunks(spec_version).await;

    let session = store.session();
    let session = session.read().await;

    // Only chunk (0,0)
    let refs = session
        .get_virtual_chunk_references(
            &Path::new("/array").unwrap(),
            &[(0, 0), (0, 0)],
        )
        .await?;

    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].location, "s3://bucket/file0.nc");
    assert_eq!(refs[0].offset, 0);
    assert_eq!(refs[0].length, 1000);

    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_get_virtual_chunk_references_partial_row(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let (store, _tmp) =
        setup_store_with_virtual_chunks(spec_version).await;

    let session = store.session();
    let session = session.read().await;

    // Row 0, both columns: chunks (0,0) and (0,1)
    let refs = session
        .get_virtual_chunk_references(
            &Path::new("/array").unwrap(),
            &[(0, 0), (0, 1)],
        )
        .await?;

    assert_eq!(refs.len(), 2);
    let locations: Vec<&str> =
        refs.iter().map(|r| r.location.as_str()).collect();
    assert!(locations.contains(&"s3://bucket/file0.nc"));
    assert!(locations.contains(&"s3://bucket/file1.nc"));

    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_get_virtual_chunk_references_materialized_excluded(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let (store, _tmp) =
        setup_store_with_virtual_chunks(spec_version).await;

    let session = store.session();
    let session = session.read().await;

    // Only chunk (1,1) which is materialized
    let refs = session
        .get_virtual_chunk_references(
            &Path::new("/array").unwrap(),
            &[(1, 1), (1, 1)],
        )
        .await?;

    assert_eq!(refs.len(), 0);

    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_get_virtual_chunk_references_uncommitted(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let (repo, _tmp) = create_repo(spec_version).await;
    let ds = repo.writable_session("main").await.unwrap();
    let store =
        Store::from_session(Arc::new(RwLock::new(ds))).await;

    // Set up zarr v3 group + array
    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group"}"#,
            ),
        )
        .await
        .unwrap();
    let zarr_meta = Bytes::copy_from_slice(
        br#"{"zarr_format":3,"node_type":"array","shape":[10],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[5]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#,
    );
    store.set("arr/zarr.json", zarr_meta).await.unwrap();

    // Set virtual ref but DON'T commit
    let vref = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "s3://bucket/uncommitted.nc",
        )
        .unwrap(),
        offset: 42,
        length: 999,
        checksum: None,
    };
    store
        .set_virtual_ref("arr/c/0", vref, false)
        .await
        .unwrap();

    let session = store.session();
    let session = session.read().await;

    let refs = session
        .get_virtual_chunk_references(&Path::new("/arr").unwrap(), &[(0, 1)])
        .await?;

    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0],
        VirtualObjectReference {
            location: "s3://bucket/uncommitted.nc".to_string(),
            offset: 42,
            length: 999,
        }
    );

    Ok(())
}
