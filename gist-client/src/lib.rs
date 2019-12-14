//! Gist client.

use chrono::{DateTime, Utc};
use http::{
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH},
    HeaderValue, Request, StatusCode,
};
use isahc::RequestExt;
use mime::Mime;
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use std::collections::HashMap;

/// The entity tag to specify the revision of Gist content.
#[derive(Debug, Clone)]
pub struct ETag(HeaderValue);

/// Gist client.
#[derive(Debug)]
pub struct Client {
    token: Option<String>,
}

impl Client {
    /// Create a new Gist client.
    pub fn new(token: Option<String>) -> Self {
        Self { token }
    }

    /// Fetch a single gist with the specific ID.
    ///
    /// https://developer.github.com/v3/gists/#get-a-single-gist
    pub async fn fetch_gist(
        &self,
        gist_id: &str,
        etag: Option<&ETag>,
    ) -> anyhow::Result<Option<(Gist, Option<ETag>)>> {
        let response = {
            let url = format!("https://api.github.com/gists/{id}", id = gist_id);
            let mut request = Request::get(url);
            // TODO: specify the custom media types
            // https://developer.github.com/v3/gists/#custom-media-types
            request.header(ACCEPT, "application/vnd.github.v3+json");
            if let Some(ref token) = self.token {
                request.header(AUTHORIZATION, format!("token {token}", token = token));
            }

            if let Some(etag) = etag {
                request.header(IF_NONE_MATCH, &etag.0);
            }

            request.body(())?.send_async().await?
        };

        match response.status() {
            StatusCode::OK => (),
            StatusCode::NOT_MODIFIED => return Ok(None),
            StatusCode::NOT_FOUND => return Err(anyhow::anyhow!("The Gist is not found")),
            status => return Err(anyhow::anyhow!("API error: {}", status)),
        }

        if let Some(content_type) = response.headers().get(CONTENT_TYPE) {
            let mime: Mime = content_type.to_str()?.parse()?;
            anyhow::ensure!(
                mime.type_() == "application" && mime.subtype() == "json",
                "content type is not JSON"
            );
        }

        let etag = response.headers().get(ETAG).map(|etag| ETag(etag.clone()));

        let body = response.into_body().text_async().await?;
        let gist: Gist = serde_json::from_str(&body)?;

        anyhow::ensure!(gist.id == gist_id, "Gist ID is mismatched");

        Ok(Some((gist, etag)))
    }

    /// Edit the content of a Gist file.
    ///
    /// https://developer.github.com/v3/gists/#edit-a-gist
    pub async fn update_gist(
        &self,
        gist_id: &str,
        etag: Option<&ETag>,
        patch: GistPatch<'_>,
    ) -> anyhow::Result<(Gist, Option<ETag>)> {
        let response = {
            let url = format!("https://api.github.com/gists/{id}", id = gist_id);
            let mut request = Request::patch(url);
            // TODO: specify the custom media types
            // https://developer.github.com/v3/gists/#custom-media-types
            request.header(ACCEPT, "application/vnd.github.v3+json");
            request.header(CONTENT_TYPE, "application/json; charset=utf-8");
            if let Some(ref token) = self.token {
                request.header(AUTHORIZATION, format!("token {token}", token = token));
            }

            if let Some(etag) = etag {
                request.header(IF_MATCH, &etag.0);
            }

            request
                .body(serde_json::to_string(&patch)?)?
                .send_async()
                .await?
        };

        match response.status() {
            StatusCode::OK => (),
            StatusCode::NOT_FOUND => return Err(anyhow::anyhow!("The Gist is not found")),
            StatusCode::PRECONDITION_FAILED => {
                return Err(anyhow::anyhow!("The Gist has been edited by someone."))
            }
            status => return Err(anyhow::anyhow!("API error: {}", status)),
        }

        if let Some(content_type) = response.headers().get(CONTENT_TYPE) {
            let mime: Mime = content_type.to_str()?.parse()?;
            anyhow::ensure!(
                mime.type_() == "application" && mime.subtype() == "json",
                "content type is not JSON"
            );
        }

        let etag = response.headers().get(ETAG).map(|etag| ETag(etag.clone()));

        let body = response.into_body().text_async().await?;
        let gist: Gist = serde_json::from_str(&body)?;

        anyhow::ensure!(gist.id == gist_id, "Gist ID is mismatched");

        Ok((gist, etag))
    }
}

/// A Gist received from the server.
#[derive(Debug, Deserialize)]
pub struct Gist {
    pub id: String,
    pub description: String,
    pub public: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub files: HashMap<String, GistFile>,

    /// Indicates that the entire file list is truncated since the total
    /// number of files is larger than 300.
    ///
    /// See [the trunctation section](https://developer.github.com/v3/gists/#truncation) for details.
    pub truncated: bool,
}

/// A file contained in a Gist.
#[derive(Debug, Deserialize)]
pub struct GistFile {
    pub filename: String,
    #[serde(rename = "type", deserialize_with = "parse_mime")]
    pub type_: Mime,
    pub language: String,
    pub raw_url: String,
    pub size: u64,
    pub truncated: bool,
    pub content: String,
}

fn parse_mime<'de, D>(de: D) -> Result<Mime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(de)?;
    s.parse().map_err(serde::de::Error::custom)
}

pub struct GistPatch<'a> {
    pub files: &'a [(&'a str, Option<&'a str>)],
    pub description: Option<&'a str>,
}

impl Serialize for GistPatch<'_> {
    fn serialize<S>(&self, se: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = se.serialize_map(Some(2))?;
        map.serialize_entry("files", &self.files)?;
        if let Some(description) = self.description {
            map.serialize_entry("description", description)?;
        }
        map.end()
    }
}

struct GistPatchFiles<'a>(&'a [(&'a str, Option<&'a str>)]);

impl Serialize for GistPatchFiles<'_> {
    fn serialize<S>(&self, se: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = se.serialize_map(Some(self.0.len()))?;
        for &(filename, content) in self.0 {
            map.serialize_entry(
                filename,
                &content.map(|content| GistPatchFile { filename, content }),
            )?;
        }
        map.end()
    }
}

#[derive(Serialize)]
struct GistPatchFile<'a> {
    filename: &'a str,
    content: &'a str,
}
