use crate::jinja::DbtRef;
use crate::project::ProjectManifest;
use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use ropey::Rope;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use std::sync::OnceLock;
use regex::Regex;

pub fn validate_refs(
    refs: &[(DbtRef, std::ops::Range<usize>)],
    manifest: Option<&ProjectManifest>,
    rope: &Rope,
    _tree: Option<&tree_sitter::Tree>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    
    // 1. Syntax Errors from sqlparser-rs
    // Skip syntax validation for macro files (they aren't pure SQL)
    let text = rope.to_string();
    if crate::jinja::is_macro_file(&text) {
        return diagnostics;
    }

    let preprocessed = crate::jinja::preprocess_for_parsing(&text);
    if let Err(e) = Parser::parse_sql(&BigQueryDialect {}, &preprocessed) {
        if let Some(diag) = parse_sqlparser_error(e, rope) {
            diagnostics.push(diag);
        }
    }

    // 2. Ref Validation (Semantic)
    if let Some(manifest) = manifest {
        for (dbt_ref, range) in refs {
            let is_valid = match dbt_ref {
                DbtRef::Model(name) => manifest.models.contains_key(name) || manifest.seeds.contains_key(name),
                DbtRef::Source(src, tbl) => manifest.sources.contains_key(&format!("{}.{}", src, tbl)),
                DbtRef::Macro(name) => manifest.macros.contains_key(name),
            };

            if !is_valid {
                let start_line = rope.byte_to_line(range.start);
                let start_char = range.start - rope.line_to_byte(start_line);
                let end_line = rope.byte_to_line(range.end);
                let end_char = range.end - rope.line_to_byte(end_line);

                let msg = match dbt_ref {
                    DbtRef::Model(name) => format!("Model/Seed '{}' not found in project.", name),
                    DbtRef::Source(s, t) => format!("Source '{}.{}' not found.", s, t),
                    DbtRef::Macro(name) => format!("Macro '{}' not found in project.", name),
                };

                diagnostics.push(Diagnostic {
                    range: Range {
                        start: Position::new(start_line as u32, start_char as u32),
                        end: Position::new(end_line as u32, end_char as u32),
                    },
                    severity: Some(DiagnosticSeverity::ERROR),
                    code: None,
                    code_description: None,
                    source: Some("dbt-lsp".to_string()),
                    message: msg,
                    related_information: None,
                    tags: None,
                    data: None,
                });
            }
        }
    }

    diagnostics
}

fn parse_sqlparser_error(err: sqlparser::parser::ParserError, _rope: &Rope) -> Option<Diagnostic> {
    let msg = format!("{}", err);
    
    // sqlparser errors can have various formats:
    static RE_POS: OnceLock<Regex> = OnceLock::new();
    let re = RE_POS.get_or_init(|| Regex::new(r#"(?i)Line:?\s*(\d+),?\s*Column:?\s*(\d+)"#).unwrap());

    let (mut line, mut col) = (0, 0);
    if let Some(cap) = re.captures(&msg) {
        line = cap[1].parse::<u32>().unwrap_or(1).saturating_sub(1);
        col = cap[2].parse::<u32>().unwrap_or(1).saturating_sub(1);
    }

    Some(Diagnostic {
        range: Range {
            start: Position::new(line, col),
            end: Position::new(line, col + 1), // Highlight at least one char
        },
        severity: Some(DiagnosticSeverity::ERROR),
        message: msg,
        source: Some("sqlparser".to_string()),
        ..Diagnostic::default()
    })
}
