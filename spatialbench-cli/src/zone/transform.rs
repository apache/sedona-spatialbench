use anyhow::Result;
use arrow_schema::Schema;
use datafusion::{prelude::*, sql::TableReference};
use log::{debug, info};

pub struct ZoneTransformer {
    offset: i64,
}

impl ZoneTransformer {
    pub fn new(offset: i64) -> Self {
        Self { offset }
    }

    pub async fn transform(&self, ctx: &SessionContext, df: DataFrame) -> Result<DataFrame> {
        ctx.register_table(TableReference::bare("zone_filtered"), df.into_view())?;
        debug!("Registered filtered data as 'zone_filtered' table");

        let sql = format!(
            r#"
            SELECT
              CAST(ROW_NUMBER() OVER (ORDER BY id) + {} AS BIGINT) AS z_zonekey,
              COALESCE(id, '')            AS z_gersid,
              COALESCE(country, '')       AS z_country,
              COALESCE(region,  '')       AS z_region,
              COALESCE(names.primary, '') AS z_name,
              COALESCE(subtype, '')       AS z_subtype,
              geometry                    AS z_boundary
            FROM zone_filtered
            "#,
            self.offset
        );

        debug!("Executing SQL transformation with offset: {}", self.offset);
        let df = ctx.sql(&sql).await?;
        info!("SQL transformation completed successfully");

        Ok(df)
    }

    pub fn arrow_schema(&self, df: &DataFrame) -> Result<Schema> {
        Ok(Schema::new(
            df.schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ))
    }
}
