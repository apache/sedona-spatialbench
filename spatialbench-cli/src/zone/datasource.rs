use anyhow::Result;
use datafusion::{
    common::config::ConfigOptions,
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    prelude::*,
};
use log::{debug, info};
use object_store::http::HttpBuilder;
use std::sync::Arc;
use url::Url;

use super::stats::ZoneTableStats;

const OVERTURE_RELEASE_DATE: &str = "2025-08-20.1";
const HUGGINGFACE_URL: &str = "https://huggingface.co";
const COMMIT_HASH: &str = "67822daa2fbc0039681922f0d7fea4157f41d13f";
const PARQUET_PART_COUNT: usize = 4;
const PARQUET_UUID: &str = "c998b093-fa14-440c-98f0-bbdb2126ed22";

pub struct ZoneDataSource {
    runtime: Arc<RuntimeEnv>,
}

impl ZoneDataSource {
    pub async fn new() -> Result<Self> {
        let rt = Arc::new(RuntimeEnvBuilder::new().build()?);

        let hf_store = HttpBuilder::new().with_url(HUGGINGFACE_URL).build()?;
        let hf_url = Url::parse(HUGGINGFACE_URL)?;
        rt.register_object_store(&hf_url, Arc::new(hf_store));

        debug!("Registered HTTPS object store for huggingface.co");

        Ok(Self { runtime: rt })
    }

    pub fn create_context(&self) -> Result<SessionContext> {
        let mut cfg = ConfigOptions::new();

        // Avoid parallelism to ensure ordering of source data
        cfg.execution.target_partitions = 1;

        let ctx =
            SessionContext::new_with_config_rt(SessionConfig::from(cfg), Arc::clone(&self.runtime));

        debug!("Created DataFusion session context");
        Ok(ctx)
    }

    pub async fn load_zone_data(
        &self,
        ctx: &SessionContext,
        scale_factor: f64,
    ) -> Result<DataFrame> {
        let parquet_urls = self.generate_parquet_urls();
        info!(
            "Reading {} Parquet parts from Hugging Face...",
            parquet_urls.len()
        );

        let df = ctx
            .read_parquet(parquet_urls, ParquetReadOptions::default())
            .await?;

        let stats = ZoneTableStats::new(scale_factor, 1);
        let subtypes = stats.subtypes();

        info!("Selected subtypes for SF {}: {:?}", scale_factor, subtypes);

        let mut pred = col("subtype").eq(lit("__never__"));
        for s in subtypes {
            pred = pred.or(col("subtype").eq(lit(s)));
        }

        let df = df.filter(pred.and(col("is_land").eq(lit(true))))?;
        info!("Applied subtype and is_land filters");

        // Sort by 'id' to ensure deterministic ordering regardless of parallelism
        // let df = df.sort(vec![col("id").sort(true, false)])?;
        // info!("Sorted by id for deterministic ordering");

        Ok(df)
    }

    fn generate_parquet_urls(&self) -> Vec<String> {
        (0..PARQUET_PART_COUNT)
            .map(|i| {
                format!(
                    "https://huggingface.co/datasets/apache-sedona/spatialbench/resolve/{}/omf-division-area-{}/part-{:05}-{}-c000.zstd.parquet",
                    COMMIT_HASH, OVERTURE_RELEASE_DATE, i, PARQUET_UUID
                )
            })
            .collect()
    }
}
