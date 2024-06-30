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

use super::core::{AliyunDriveCore, UploadUrlResponse};
use crate::{
    raw::*,
    services::aliyun_drive::core::{CheckNameMode, CreateResponse, CreateType},
    *,
};
use bytes::Buf;
use std::sync::Arc;

pub struct AliyunDriveWriter {
    core: Arc<AliyunDriveCore>,

    _op: OpWrite,
    parent_file_id: String,
    name: String,

    file_id: Option<String>,
    upload_id: Option<String>,
    part_number: usize,
}

impl AliyunDriveWriter {
    pub fn new(core: Arc<AliyunDriveCore>, parent_file_id: &str, name: &str, op: OpWrite) -> Self {
        AliyunDriveWriter {
            core,
            _op: op,
            parent_file_id: parent_file_id.to_string(),
            name: name.to_string(),
            file_id: None,
            upload_id: None,
            part_number: 1, // must start from 1
        }
    }

    async fn get_upload_info(&mut self, do_not_create: bool) -> Result<(String, String)> {
        let (upload_id, file_id) = match (self.upload_id.clone(), self.file_id.clone()) {
            (Some(upload_id), Some(file_id)) => (upload_id, file_id),
            _ if do_not_create => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "cannot find upload_id and file_id",
                ));
            }
            _ => {
                let res = self
                    .core
                    .create(
                        Some(&self.parent_file_id),
                        &self.name,
                        CreateType::File,
                        CheckNameMode::Refuse,
                    )
                    .await?;
                let output: CreateResponse =
                    serde_json::from_reader(res.reader()).map_err(new_json_deserialize_error)?;
                if output.exist.is_some_and(|x| x) {
                    return Err(Error::new(ErrorKind::AlreadyExists, "file exists"));
                }
                let upload_id = output.upload_id.expect("cannot find upload_id");
                let file_id = output.file_id;
                self.upload_id = Some(upload_id.clone());
                self.file_id = Some(file_id.clone());
                (upload_id, file_id)
            }
        };

        Ok((upload_id, file_id))
    }
}

impl oio::Write for AliyunDriveWriter {
    async fn write(&mut self, bs: Buffer) -> Result<usize> {
        let (upload_id, file_id) = self.get_upload_info(false).await?;

        let res = self
            .core
            .get_upload_url(&file_id, &upload_id, Some(self.part_number))
            .await?;
        let output: UploadUrlResponse =
            serde_json::from_reader(res.reader()).map_err(new_json_deserialize_error)?;

        let upload_url = match output.part_info_list {
            Some(part_info_list) if !part_info_list.is_empty() => {
                part_info_list[0].upload_url.to_owned()
            }
            _ => {
                return Err(Error::new(ErrorKind::Unexpected, "cannot find upload_url"));
            }
        };

        let size = bs.len();

        if let Err(err) = self.core.upload(&upload_url, bs).await {
            if err.kind() != ErrorKind::AlreadyExists {
                return Err(err);
            }
        };

        self.part_number += 1;

        Ok(size)
    }

    async fn close(&mut self) -> Result<()> {
        let (upload_id, file_id) = self.get_upload_info(true).await?;
        self.core.complete(&file_id, &upload_id).await?;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        let (_, file_id) = self.get_upload_info(true).await?;
        self.core.delete_path(&file_id).await
    }
}
