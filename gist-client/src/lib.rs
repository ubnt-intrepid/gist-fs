//! Gist client.

use chrono::{DateTime, Utc};
use http::{Request, StatusCode};
use isahc::RequestExt;
use mime::Mime;
use serde::Deserialize;
use std::collections::HashMap;

/// Gist client.
#[derive(Debug)]
pub struct GistClient {
    id: String,
    token: Option<String>,
}

impl GistClient {
    pub fn new(id: String) -> Self {
        Self { id, token: None }
    }

    pub fn set_token(&mut self, token: String) {
        self.token.replace(token);
    }

    pub async fn fetch(&self) -> anyhow::Result<Gist> {
        let response = {
            let url = format!("https://api.github.com/gists/{id}", id = self.id);
            let mut request = Request::get(url);
            if let Some(ref token) = self.token {
                request.header(
                    http::header::AUTHORIZATION,
                    format!("token {token}", token = token),
                );
            }
            request.body(())?.send_async().await?
        };

        match response.status() {
            StatusCode::OK => (),
            StatusCode::NOT_FOUND => return Err(anyhow::anyhow!("The Gist is not found")),
            status => return Err(anyhow::anyhow!("API error: {}", status)),
        }

        let body = response.into_body().text_async().await?;
        let gist: Gist = serde_json::from_str(&body)?;
        anyhow::ensure!(gist.id == self.id, "Gist ID is mismatched");

        Ok(gist)
    }
}

#[derive(Debug, Deserialize)]
pub struct Gist {
    pub id: String,
    pub description: String,
    pub public: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub files: HashMap<String, GistFile>,
}

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
