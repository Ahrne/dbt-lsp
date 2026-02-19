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
        let (diagnostics, ctes, aliases) = crate::diagnostics::validate_refs(&refs, manifest_guard.as_deref(), &rope, tree.as_ref());
        
        // 4. Update State
        self.state.documents.insert(uri.clone(), crate::state::DocumentState {
            text: rope.clone(),
            tree,
            refs: refs.clone(),
            ctes,
            aliases,
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
             let (diagnostics, ctes, aliases) = crate::diagnostics::validate_refs(&refs, manifest_guard.as_deref(), &rope, tree.as_ref());
             
             if let Some(mut doc) = self.state.documents.get_mut(&uri) {
                 doc.tree = tree;
                 doc.refs = refs.clone();
                 doc.ctes = ctes;
                 doc.aliases = aliases;
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

             // 1. Check for CTEs (local definitions)
             if let Some(word) = get_word_at_pos(&doc.text, char_idx) {
                 if let Some(cte_def) = doc.ctes.get(&word) {
                     let range = &cte_def.name_range;
                     let start_line = doc.text.byte_to_line(range.start);
                     let start_char = range.start - doc.text.line_to_byte(start_line);
                     let end_line = doc.text.byte_to_line(range.end);
                     let end_char = range.end - doc.text.line_to_byte(end_line);
                     
                     self.client.log_message(MessageType::INFO, format!("Found CTE definition: {}", word)).await;
                     return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                         uri: uri.clone(),
                         range: Range {
                             start: Position::new(start_line as u32, start_char as u32),
                             end: Position::new(end_line as u32, end_char as u32),
                         },
                     })));
                 }
             }

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
                                   } else if let Some(path) = manifest.seeds.get(name) {
                                       let target_uri = Url::from_file_path(path.value()).unwrap();
                                       return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                                           uri: target_uri,
                                           range: Range::default(),
                                       })));
                                   } else {
                                       self.client.show_message(MessageType::WARNING, format!("Model/Seed '{}' not found in project manifest", name)).await;
                                   }
                               } else {
                                   self.client.show_message(MessageType::ERROR, "Project manifest not loaded!").await;
                               }
                          },
                          crate::jinja::DbtRef::Source(src, tbl) => {
                               let manifest = self.state.manifest.read().await;
                               if let Some(manifest) = manifest.as_ref() {
                                   let full_name = format!("{}.{}", src, tbl);
                                   if let Some(path) = manifest.sources.get(&full_name) {
                                       let target_uri = Url::from_file_path(path.value()).unwrap();
                                       return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                                           uri: target_uri,
                                           range: Range::default(),
                                       })));
                                   } else {
                                       self.client.show_message(MessageType::WARNING, format!("Source '{}.{}' not found in manifest", src, tbl)).await;
                                   }
                               }
                          },
                          crate::jinja::DbtRef::Macro(name) => {
                               let manifest = self.state.manifest.read().await;
                               if let Some(manifest) = manifest.as_ref() {
                                   if let Some(m_def) = manifest.macros.get(name) {
                                       let target_uri = Url::from_file_path(&m_def.path).unwrap();
                                       return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                                           uri: target_uri,
                                           range: Range {
                                               start: Position::new(m_def.line as u32, 0),
                                               end: Position::new(m_def.line as u32, 0),
                                           },
                                       })));
                                   }
                               }
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
        
        self.client.log_message(MessageType::LOG, format!("Hover request at Line: {}, Col: {}", position.line, position.character)).await;

        if let Some(doc) = self.state.documents.get(&uri) {
             let char_idx = doc.text.line_to_char(position.line as usize) + position.character as usize;
             let byte_idx = doc.text.char_to_byte(char_idx);
             eprintln!("HOVER DEBUG: byte_idx={}, refs={}", byte_idx, doc.refs.len());

             if let Some(word) = get_word_at_pos(&doc.text, char_idx) {
                 // 1. Check if word is a CTE name
                 if let Some(cte_def) = doc.ctes.get(&word) {
                     let body_slice = doc.text.slice(cte_def.body_range.clone());
                     return Ok(Some(Hover {
                         contents: HoverContents::Markup(MarkupContent {
                             kind: MarkupKind::Markdown,
                             value: format!("```sql\n{}\n```", body_slice),
                         }),
                         range: None,
                     }));
                 }
                 
                 // 2. Check if word is an Alias
                 if let Some(alias_def) = doc.aliases.get(&word) {
                     // Resolve target
                     if let Some(cte_def) = doc.ctes.get(&alias_def.target_name) {
                         // Alias points to a CTE -> show CTE body
                         let body_slice = doc.text.slice(cte_def.body_range.clone());
                         return Ok(Some(Hover {
                             contents: HoverContents::Markup(MarkupContent {
                                 kind: MarkupKind::Markdown,
                                 value: format!("**Alias for CTE** `{}`\n```sql\n{}\n```", alias_def.target_name, body_slice),
                             }),
                             range: None,
                         }));
                     } else {
                         // Alias points to something else (source/seed/model) -> show definition line
                         let source_slice = doc.text.slice(alias_def.reference_range.clone());
                          return Ok(Some(Hover {
                             contents: HoverContents::Markup(MarkupContent {
                                 kind: MarkupKind::Markdown,
                                 value: format!("**Alias Definition**:\n```sql\n{}\n```", source_slice),
                             }),
                             range: None,
                         }));
                     }
                 }
                 
                 // 3. Fallback: Check for alias.column pattern
                 if char_idx > 0 {
                      // find start of current word
                      let mut s = char_idx;
                      while s > 0 {
                          let c = doc.text.char(s - 1);
                          if !c.is_alphanumeric() && c != '_' { break; }
                          s -= 1;
                      }
                      
                      // Check for dot before word
                      if s > 0 && doc.text.char(s - 1) == '.' {
                           // Extract previous word (the alias)
                            if let Some(alias) = get_word_at_pos(&doc.text, s - 2) {
                                 if let Some(alias_def) = doc.aliases.get(&alias) {
                                     // Found alias! Resolve it.
                                     let target_desc = if let Some(cte_def) = doc.ctes.get(&alias_def.target_name) {
                                          let body_slice = doc.text.slice(cte_def.body_range.clone());
                                          format!("**Column of CTE** `{}` (alias `{}`)\n```sql\n{}\n```", alias_def.target_name, alias, body_slice)
                                     } else {
                                          let source_slice = doc.text.slice(alias_def.reference_range.clone());
                                          format!("**Column of Source** (alias `{}`)\n```sql\n{}\n```", alias, source_slice)
                                     };
                                     
                                     return Ok(Some(Hover {
                                         contents: HoverContents::Markup(MarkupContent {
                                             kind: MarkupKind::Markdown,
                                             value: target_desc,
                                         }),
                                         range: None,
                                     }));
                                 }
                            }
                      }
                 }
             }

             for (dbt_ref, range) in &doc.refs {
                 if byte_idx >= range.start && byte_idx < range.end {
                      let value = match dbt_ref {
                          crate::jinja::DbtRef::Model(name) => {
                               let manifest = self.state.manifest.read().await;
                               if let Some(m) = manifest.as_ref() {
                                   if m.seeds.contains_key(name) {
                                       format!("**Seed**: `{}`", name)
                                   } else {
                                       format!("**Model**: `{}`", name)
                                   }
                               } else {
                                   format!("**Model**: `{}`", name)
                               }
                          },
                          crate::jinja::DbtRef::Source(src, tbl) => {
                               format!("**Source**: `{}.{}`", src, tbl)
                          },
                          crate::jinja::DbtRef::Macro(name) => {
                               let manifest = self.state.manifest.read().await;
                               let mut msg = format!("**Macro**: `{}`", name);
                               if let Some(manifest) = manifest.as_ref() {
                                   if let Some(m_def) = manifest.macros.get(name) {
                                       if let Ok(content) = std::fs::read_to_string(&m_def.path) {
                                           let macro_lines: Vec<&str> = content.lines().skip(m_def.line).take(15).collect();
                                           msg.push_str("\n\n```jinja\n");
                                           msg.push_str(&macro_lines.join("\n"));
                                           msg.push_str("\n```");
                                       }
                                   }
                               }
                               msg
                          }
                      };
                      
                      return Ok(Some(Hover {
                          contents: HoverContents::Markup(MarkupContent {
                              kind: MarkupKind::Markdown,
                              value,
                          }),
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

fn get_word_at_pos(rope: &ropey::Rope, char_idx: usize) -> Option<String> {
    let len = rope.len_chars();
    if char_idx >= len { return None; }
    
    // Scan backwards
    let mut start = char_idx;
    while start > 0 {
        let c = rope.char(start - 1);
        if !c.is_alphanumeric() && c != '_' {
            break;
        }
        start -= 1;
    }
    
    // Scan forwards
    let mut end = char_idx;
    while end < len {
        let c = rope.char(end);
        if !c.is_alphanumeric() && c != '_' {
            break;
        }
        end += 1;
    }
    
    if start == end { return None; }
    
    Some(rope.slice(start..end).to_string())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--test") {
        eprintln!("dbt-lsp binary check: OK");
        return;
    }

    env_logger::init();
    eprintln!("dbt-lsp starting...");

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| Backend { 
        client,
        state: GlobalState::default(),
    });
    Server::new(stdin, stdout, socket).serve(service).await;
}
