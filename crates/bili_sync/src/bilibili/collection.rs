use std::fmt::{Display, Formatter};

use anyhow::{Context, Result, anyhow};
use async_stream::try_stream;
use futures::Stream;
use reqwest::Method;
use serde::Deserialize;
use serde_json::Value;

use crate::bilibili::{BiliClient, Credential, ErrorForStatusExt, Validate, VideoInfo};

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default, Copy)]
pub enum CollectionType {
    Series,
    #[default]
    Season,
}

impl<'de> serde::Deserialize<'de> for CollectionType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = i32::deserialize(deserializer)?;
        CollectionType::try_from(v).map_err(serde::de::Error::custom)
    }
}

impl From<CollectionType> for i32 {
    fn from(v: CollectionType) -> Self {
        match v {
            CollectionType::Series => 1,
            CollectionType::Season => 2,
        }
    }
}

impl TryFrom<i32> for CollectionType {
    type Error = anyhow::Error;

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(CollectionType::Series),
            2 => Ok(CollectionType::Season),
            v => Err(anyhow!("got invalid collection type {}", v)),
        }
    }
}

impl CollectionType {
    pub fn from_expected(v: i32) -> Self {
        Self::try_from(v).expect("invalid collection type")
    }
}

impl Display for CollectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            CollectionType::Series => "列表",
            CollectionType::Season => "合集",
        };
        write!(f, "{}", s)
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct CollectionItem {
    pub mid: String,
    pub sid: String,
    pub collection_type: CollectionType,
}

/// UP 主名下的一个合集/列表条目，用于扫描后选择订阅
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpperCollection {
    pub sid: i64,
    pub collection_type: CollectionType,
    pub name: String,
    pub cover: String,
    pub total: i64,
}

impl<'de> Deserialize<'de> for UpperCollection {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct UpperCollectionRaw {
            meta: CollectionMeta,
        }
        #[derive(Deserialize)]
        struct CollectionMeta {
            #[serde(default)]
            name: String,
            #[serde(default)]
            title: String,
            #[serde(default)]
            cover: String,
            #[serde(default)]
            total: i64,
            season_id: Option<i64>,
            series_id: Option<i64>,
        }
        let UpperCollectionRaw { meta } = UpperCollectionRaw::deserialize(deserializer)?;
        let (sid, collection_type) = match (meta.season_id, meta.series_id) {
            (Some(sid), None) => (sid, CollectionType::Season),
            (None, Some(sid)) => (sid, CollectionType::Series),
            _ => return Err(serde::de::Error::custom("invalid upper collection meta")),
        };
        // season 的 meta.name 带「合集·」前缀，title 是干净标题；series 只有 name
        let name = if meta.name.is_empty() { meta.title } else { meta.name };
        Ok(UpperCollection {
            sid,
            collection_type,
            name,
            cover: meta.cover,
            total: meta.total,
        })
    }
}

pub struct Collection<'a> {
    client: &'a BiliClient,
    pub collection: CollectionItem,
    credential: &'a Credential,
}

#[derive(Debug, PartialEq)]
pub struct CollectionInfo {
    pub name: String,
    pub mid: i64,
    pub sid: i64,
    pub collection_type: CollectionType,
}

impl<'de> Deserialize<'de> for CollectionInfo {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct CollectionInfoRaw {
            mid: i64,
            name: String,
            season_id: Option<i64>,
            series_id: Option<i64>,
        }
        let raw = CollectionInfoRaw::deserialize(deserializer)?;
        let (sid, collection_type) = match (raw.season_id, raw.series_id) {
            (Some(sid), None) => (sid, CollectionType::Season),
            (None, Some(sid)) => (sid, CollectionType::Series),
            _ => return Err(serde::de::Error::custom("invalid collection info")),
        };
        Ok(CollectionInfo {
            mid: raw.mid,
            name: raw.name,
            sid,
            collection_type,
        })
    }
}

impl<'a> Collection<'a> {
    pub fn new(client: &'a BiliClient, collection: CollectionItem, credential: &'a Credential) -> Self {
        Self {
            client,
            collection,
            credential,
        }
    }

    pub async fn get_info(&self) -> Result<CollectionInfo> {
        let meta = match self.collection.collection_type {
            // 没有找到专门获取 Season 信息的接口，所以直接获取第一页，从里面取 meta 信息
            CollectionType::Season => self.get_videos(1).await?["data"]["meta"].take(),
            CollectionType::Series => self.get_series_info().await?["data"]["meta"].take(),
        };
        Ok(serde_json::from_value(meta)?)
    }

    /// 拉取 UP 主名下的全部合集与列表，供页面扫描后勾选订阅。
    ///
    /// 已知限制（未实现，后续也不打算做）：
    /// 这里只做「列出 UP 主有哪些合集」，不会与本地已下载的文件做任何比对或合并。
    /// 添加合集后，入库依赖 bvid 唯一索引去重（见 utils/model.rs::create_videos），
    /// 因此只有爬到的条目才会进库；本地磁盘上已经存在、但数据库里没有记录的视频，
    /// 不会被识别、也不会自动补登记，仍需要靠后续正常同步重新抓取一遍元数据。
    /// 想要「扫描本地文件直接入库」需要额外实现：解析文件名/目录结构反推 bvid，
    /// 再校验时长或分P，成本高且容易误判，收益有限，故不实现。
    pub async fn list_by_upper(
        client: &'a BiliClient,
        credential: &'a Credential,
        mid: &str,
    ) -> Result<Vec<UpperCollection>> {
        let mut page = 1;
        let mut result = Vec::new();
        loop {
            let res = client
                .request(
                    Method::GET,
                    "https://api.bilibili.com/x/polymer/web-space/seasons_series_list",
                    credential,
                )
                .await
                .query(&[("mid", mid), ("page_num", &page.to_string()), ("page_size", "20")])
                .send()
                .await?
                .error_for_status_ext()?
                .json::<Value>()
                .await?
                .validate()?;
            let lists = &res["data"]["items_lists"];
            let mut got = 0usize;
            for key in ["seasons_list", "series_list"] {
                let Some(items) = lists[key].as_array() else {
                    continue;
                };
                got += items.len();
                for item in items {
                    match serde_json::from_value::<UpperCollection>(item.clone()) {
                        Ok(collection) => result.push(collection),
                        Err(e) => {
                            return Err(anyhow!("failed to parse collection of mid {}: {}", mid, e));
                        }
                    }
                }
            }
            let page_info = &lists["page"];
            let (Some(num), Some(size), Some(total)) = (
                page_info["page_num"].as_i64(),
                page_info["page_size"].as_i64(),
                page_info["total"].as_i64(),
            ) else {
                return Err(anyhow!(
                    "invalid page info when listing collections of mid {}: read {}",
                    mid,
                    page_info
                ));
            };
            if got == 0 || num * size >= total {
                break;
            }
            page += 1;
        }
        Ok(result)
    }

    async fn get_series_info(&self) -> Result<Value> {
        self.client
            .request(Method::GET, "https://api.bilibili.com/x/series/series", self.credential)
            .await
            .query(&[("series_id", self.collection.sid.as_str())])
            .send()
            .await?
            .error_for_status_ext()?
            .json::<Value>()
            .await?
            .validate()
    }

    async fn get_videos(&self, page: i32) -> Result<Value> {
        let req = match self.collection.collection_type {
            CollectionType::Series => self
                .client
                .request(
                    Method::GET,
                    "https://api.bilibili.com/x/series/archives",
                    self.credential,
                )
                .await
                .query(&[("pn", page)])
                .query(&[
                    ("mid", self.collection.mid.as_str()),
                    ("series_id", self.collection.sid.as_str()),
                    ("only_normal", "true"),
                    ("sort", "desc"),
                    ("ps", "30"),
                ]),
            CollectionType::Season => self
                .client
                .request(
                    Method::GET,
                    "https://api.bilibili.com/x/polymer/web-space/seasons_archives_list",
                    self.credential,
                )
                .await
                .query(&[("page_num", page)])
                .query(&[
                    ("mid", self.collection.mid.as_str()),
                    ("season_id", self.collection.sid.as_str()),
                    ("sort_reverse", "true"),
                    ("page_size", "30"),
                ]),
        };
        req.send()
            .await?
            .error_for_status_ext()?
            .json::<Value>()
            .await?
            .validate()
    }

    pub fn into_video_stream(self) -> impl Stream<Item = Result<VideoInfo>> + 'a {
        try_stream! {
            let mut page = 1;
            loop {
                let mut videos = self.get_videos(page).await.with_context(|| {
                    format!(
                        "failed to get videos of collection {:?} page {}",
                        self.collection, page
                    )
                })?;
                let archives = &mut videos["data"]["archives"];
                if archives.as_array().is_none_or(|v| v.is_empty()) {
                    if page == 1 {
                        break;
                    }
                    Err(anyhow!(
                        "no videos found in collection {:?} page {}",
                        self.collection,
                        page
                    ))?;
                }
                let videos_info: Vec<VideoInfo> = serde_json::from_value(archives.take()).with_context(|| {
                    format!(
                        "failed to parse videos of collection {:?} page {}",
                        self.collection, page
                    )
                })?;
                for video_info in videos_info {
                    yield video_info;
                }
                let page_info = &videos["data"]["page"];
                let fields = match self.collection.collection_type {
                    CollectionType::Series => ["num", "size", "total"],
                    CollectionType::Season => ["page_num", "page_size", "total"],
                };
                let values = fields
                    .iter()
                    .map(|f| page_info[f].as_i64())
                    .collect::<Vec<Option<i64>>>();
                if let [Some(num), Some(size), Some(total)] = values[..] {
                    if num * size < total {
                        page += 1;
                        continue;
                    }
                } else {
                    Err(anyhow!(
                        "invalid page info of collection {:?} page {}: read {:?} from {}",
                        self.collection,
                        page,
                        fields,
                        page_info
                    ))?;
                }
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_info_parse() {
        let testcases = vec![
            (
                r#"
                    {
                        "category": 0,
                        "cover": "https://archive.biliimg.com/bfs/archive/a6fbf7a7b9f4af09d9cf40482268634df387ef68.jpg",
                        "description": "",
                        "mid": 521722088,
                        "name": "合集·【命运方舟全剧情解说】",
                        "ptime": 1714701600,
                        "season_id": 1987140,
                        "total": 10
                    }
                "#,
                CollectionInfo {
                    mid: 521722088,
                    name: "合集·【命运方舟全剧情解说】".to_owned(),
                    sid: 1987140,
                    collection_type: CollectionType::Season,
                },
            ),
            (
                r#"
                    {
                        "series_id": 387212,
                        "mid": 521722088,
                        "name": "提瓦特冒险记",
                        "description": "原神沙雕般的游戏体验",
                        "keywords": [
                            ""
                        ],
                        "creator": "",
                        "state": 2,
                        "last_update_ts": 1633167320,
                        "total": 3,
                        "ctime": 1633167320,
                        "mtime": 1633167320,
                        "raw_keywords": "",
                        "category": 1
                    }
                "#,
                CollectionInfo {
                    mid: 521722088,
                    name: "提瓦特冒险记".to_owned(),
                    sid: 387212,
                    collection_type: CollectionType::Series,
                },
            ),
        ];
        for (json, expect) in testcases {
            let info: CollectionInfo = serde_json::from_str(json).unwrap();
            assert_eq!(info, expect);
        }
    }
}
