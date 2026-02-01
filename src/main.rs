mod project;
mod state;
mod parser;
mod jinja;
mod diagnostics;

use crate::state::GlobalState;
use std::sync::Arc;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

#[derive(Debug)]
struct Backend {
    client: Client,
    state: GlobalState,
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        self.client
            .log_message(MessageType::INFO, "dbt-lsp initializing...")
            .await;
            
        // Support both deprecated root_uri and modern workspace_folders
        let root_path = params.root_uri.and_then(|u| u.to_file_path().ok())
            .or_else(|| {
                params.workspace_folders.as_ref().and_then(|folders| {
                    folders.get(0).and_then(|f| f.uri.to_file_path().ok())
                })
            });

        if let Some(path) = root_path {
            self.client.log_message(MessageType::INFO, format!("Initializing at root: {:?}", path)).await;
            match crate::project::ProjectManifest::load(path) {
                Ok(manifest) => {
                    let model_count = manifest.models.len();
                    let msg = format!("Loaded dbt project: {} with {} models", manifest.config.name, model_count);
                    self.client.log_message(MessageType::INFO, msg.clone()).await;
                    self.client.show_message(MessageType::INFO, msg).await;
                    *self.state.manifest.write().await = Some(Arc::new(manifest));
                }
                Err(e) => {
                    let msg = format!("Failed to load dbt project: {}", e);
                    self.client.log_message(MessageType::ERROR, msg.clone()).await;
                    self.client.show_message(MessageType::ERROR, msg).await;
                }
            }
        } else {
            self.client.show_message(MessageType::WARNING, "No root directory detected. Manifest loading skipped.").await;
        }

        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::INCREMENTAL,
                )),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                definition_provider: Some(OneOf::Left(true)),
                completion_provider: Some(CompletionOptions {
                    trigger_characters: Some(vec!["'".to_string(), "\"".to_string()]),
                    ..CompletionOptions::default()
                }),
                ..ServerCapabilities::default()
            },
            ..InitializeResult::default()
        })
    }

    async fn shutdown(&self) -> Result<()> {
        self.client
            .log_message(MessageType::INFO, "dbt-lsp shutting down...")
            .await;
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let text = params.text_document.text;
        
        // 1. Preprocess for parsing (preserves length)
        let preprocessed = crate::jinja::preprocess_for_parsing(&text);
        
        // 2. Parse (using preprocessed text)
        let tree = if let Ok(mut parser) = crate::parser::DbtParser::new() {
             parser.parse(&preprocessed, None)
        } else {
             None
        };
        
        // 3. Extract Refs (using original text for semantics)
        let refs = crate::jinja::extract_refs(&text);
        
        let rope = ropey::Rope::from_str(&text);

        // 5. Generate and Publish Diagnostics
        let manifest_guard = self.state.manifest.read().await;
        let diagnostics = crate::diagnostics::validate_refs(&refs, manifest_guard.as_deref(), &rope, tree.as_ref());
        
        // 4. Update State
        self.state.documents.insert(uri.clone(), crate::state::DocumentState {
            text: rope.clone(),
            tree,
            refs: refs.clone(),
            diagnostics: Vec::new(), 
        });

        self.client.publish_diagnostics(uri, diagnostics, None).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        
        // Scope for mutable access to update text
        let full_text = {
            if let Some(mut doc) = self.state.documents.get_mut(&uri) {
                for change in params.content_changes {
                    if let Some(range) = change.range {
                        let start_char_idx = doc.text.line_to_char(range.start.line as usize) + range.start.character as usize;
                        let end_char_idx = doc.text.line_to_char(range.end.line as usize) + range.end.character as usize;
                        
                        if start_char_idx <= doc.text.len_chars() && end_char_idx <= doc.text.len_chars() {
                            doc.text.remove(start_char_idx..end_char_idx);
                            doc.text.insert(start_char_idx, &change.text);
                        }
                    } else {
                        doc.text = ropey::Rope::from_str(&change.text);
                    }
                }
                Some(doc.text.to_string())
            } else {
                None
            }
        };

        if let Some(text) = full_text {
             // 1. Preprocess
             let preprocessed = crate::jinja::preprocess_for_parsing(&text);
             
             // 2. Parse
             let tree = if let Ok(mut parser) = crate::parser::DbtParser::new() {
                 parser.parse(&preprocessed, None)
             } else {
                 None
             };
             
             // 3. Extract Refs
             let refs = crate::jinja::extract_refs(&text);
             
             // 4. Update State (need to re-acquire write lock or update via dashmap)
             // We need rope for diagnostics, can create from text or clone from state
             // But avoiding excessive locking/cloning.
             // We can just get rope from state again or use the text we have.
             let rope = ropey::Rope::from_str(&text);
             
             // 5. Generate and Publish Diagnostics
             let manifest_guard = self.state.manifest.read().await;
             let diagnostics = crate::diagnostics::validate_refs(&refs, manifest_guard.as_deref(), &rope, tree.as_ref());
             
             if let Some(mut doc) = self.state.documents.get_mut(&uri) {
                 doc.tree = tree;
                 doc.refs = refs.clone();
             }

             self.client.publish_diagnostics(uri, diagnostics, None).await;
        }
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        self.client.log_message(MessageType::INFO, format!("GotoDef request at {:?} in {}", position, uri)).await;

        if let Some(doc) = self.state.documents.get(&uri) {
             let line_idx = position.line as usize;
             if line_idx >= doc.text.len_lines() {
                 return Ok(None);
             }

             let line_start_char = doc.text.line_to_char(line_idx);
             let char_idx = line_start_char + position.character as usize;
             
             if char_idx >= doc.text.len_chars() {
                  return Ok(None);
             }
             let byte_idx = doc.text.char_to_byte(char_idx);

             self.client.log_message(MessageType::INFO, format!("Byte idx: {}. Refs: {}", byte_idx, doc.refs.len())).await;

             for (dbt_ref, range) in &doc.refs {
                 // Use < range.end to avoid character-after-match hits
                 if byte_idx >= range.start && byte_idx < range.end {
                      self.client.log_message(MessageType::INFO, format!("Found matching ref: {:?}", dbt_ref)).await;
                      match dbt_ref {
                          crate::jinja::DbtRef::Model(name) => {
                               let manifest = self.state.manifest.read().await;
                               if let Some(manifest) = manifest.as_ref() {
                                   if let Some(path) = manifest.models.get(name) {
                                       let target_uri = Url::from_file_path(path.value()).unwrap();
                                       return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                                           uri: target_uri,
                                           range: Range::default(),
                                       })));
                                   } else {
                                       self.client.show_message(MessageType::WARNING, format!("Model '{}' not found in project manifest", name)).await;
                                   }
                               } else {
                                   self.client.show_message(MessageType::ERROR, "Project manifest not loaded!").await;
                               }
                          },
                          crate::jinja::DbtRef::Source(src, tbl) => {
                               self.client.show_message(MessageType::INFO, format!("Source: {}.{}", src, tbl)).await;
                          } 
                      }
                 }
             }
        }
        Ok(None)
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;
        
        if let Some(doc) = self.state.documents.get(&uri) {
             let char_idx = doc.text.line_to_char(position.line as usize) + position.character as usize;
             let byte_idx = doc.text.char_to_byte(char_idx);

             for (dbt_ref, range) in &doc.refs {
                 if byte_idx >= range.start && byte_idx <= range.end {
                      let contents = match dbt_ref {
                          crate::jinja::DbtRef::Model(name) => {
                               format!("**Model**: `{}`", name)
                          },
                          crate::jinja::DbtRef::Source(src, tbl) => {
                               format!("**Source**: `{}.{}`", src, tbl)
                          }
                      };
                      
                      return Ok(Some(Hover {
                          contents: HoverContents::Scalar(MarkedString::String(contents)),
                          range: None,
                      }));
                 }
             }
        }
        Ok(None)
    }

    async fn completion(&self, _params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let manifest = self.state.manifest.read().await;
        let mut items = Vec::new();

        // 1. Keyword Snippets
        items.push(CompletionItem {
            label: "ref".to_string(),
            kind: Some(CompletionItemKind::SNIPPET),
            insert_text: Some("{{ ref('$1') }}".to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            detail: Some("Expand to ref() tag".to_string()),
            ..CompletionItem::default()
        });

        items.push(CompletionItem {
            label: "source".to_string(),
            kind: Some(CompletionItemKind::SNIPPET),
            insert_text: Some("{{ source('$1', '$2') }}".to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            detail: Some("Expand to source() tag".to_string()),
            ..CompletionItem::default()
        });

        // 2. Model names from manifest
        if let Some(manifest) = manifest.as_ref() {
            for model_ref in manifest.models.iter() {
                items.push(CompletionItem {
                    label: model_ref.key().clone(),
                    kind: Some(CompletionItemKind::FILE),
                    detail: Some("dbt model".to_string()),
                    ..CompletionItem::default()
                });
            }
        }
        
        Ok(Some(CompletionResponse::Array(items)))
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| Backend { 
        client,
        state: GlobalState::default(),
    });
    Server::new(stdin, stdout, socket).serve(service).await;
}
