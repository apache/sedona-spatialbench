// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
