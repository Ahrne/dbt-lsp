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
) -> (Vec<Diagnostic>, std::collections::HashMap<String, crate::state::CteDefinition>, std::collections::HashMap<String, crate::state::AliasDefinition>) {
    let mut diagnostics = Vec::new();
    let mut ctes = std::collections::HashMap::new();
    let mut aliases = std::collections::HashMap::new();
    
    // 1. Syntax Errors from sqlparser-rs
    // Skip syntax validation for macro files (they aren't pure SQL)
    let text = rope.to_string();
    if crate::jinja::is_macro_file(&text) {
        return (diagnostics, ctes, aliases);
    }

    // Scan for CTE definitions: "name as ("
    static RE_CTE: OnceLock<Regex> = OnceLock::new();
    let re_cte = RE_CTE.get_or_init(|| Regex::new(r#"(?i)\b([a-zA-Z0-9_]+)\s+as\s*\("#).unwrap());
    
    for cap in re_cte.captures_iter(&text) {
        if let Some(m) = cap.get(1) {
             let name = m.as_str().to_string();
             let start_body = cap.get(0).unwrap().end(); 
             if let Some(end_body) = find_closing_paren(&text, start_body) {
                 ctes.insert(name, crate::state::CteDefinition {
                     name_range: m.range(),
                     body_range: start_body..end_body,
                 });
             }
        }
    }

    // Scan for table aliases: "from/join ... as alias"
    static RE_ALIAS: OnceLock<Regex> = OnceLock::new();
    let re_alias = RE_ALIAS.get_or_init(|| Regex::new(r#"(?ix)(?:from|join)\s+(?P<source>(?:[a-zA-Z0-9_\.]+|\{\{.*?\}\}|\$\{.*?\})+)\s+(?:as\s+)?(?P<alias>[a-zA-Z0-9_]+)"#).unwrap());

    for cap in re_alias.captures_iter(&text) {
        if let Some(alias_match) = cap.name("alias") {
            let alias = alias_match.as_str().to_string();
            let lower_alias = alias.to_lowercase();
            // Filter keywords
            if matches!(lower_alias.as_str(), "on" | "where" | "group" | "order" | "limit" | "having" | "window" | "inner" | "left" | "right" | "full" | "cross" | "outer") {
                continue;
            }
            if let Some(source_match) = cap.name("source") {
                if let Some(full_match) = cap.get(0) {
                     let source_text = source_match.as_str();
                     let target_name = extract_target_name(source_text);
                     aliases.insert(alias, crate::state::AliasDefinition {
                         reference_range: source_match.range(), // Point to "source" (e.g. "{{ ref(...) }}") not full "from ... w"
                         target_name,
                     });
                }
            }
        }
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

    (diagnostics, ctes, aliases)
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

fn find_closing_paren(text: &str, start_idx: usize) -> Option<usize> {
    let mut depth = 1;
    let mut in_quote = None;
    let mut chars = text[start_idx..].char_indices();
    
    while let Some((idx, c)) = chars.next() {
        if let Some(q) = in_quote {
            if c == q {
                in_quote = None;
            }
        } else {
            match c {
                '\'' | '"' => in_quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(start_idx + idx);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn extract_target_name(source: &str) -> String {
    static RE_REF: OnceLock<Regex> = OnceLock::new();
    let re_ref = RE_REF.get_or_init(|| Regex::new(r#"(?x)(?:ref|source)\s*\(\s*['"]([^'"]+)['"](?:\s*,\s*['"]([^'"]+)['"])?\s*\)"#).unwrap());
    
    if let Some(cap) = re_ref.captures(source) {
        if let Some(m2) = cap.get(2) {
             // source('pkg', 'table') -> pkg.table
             return format!("{}.{}", cap.get(1).unwrap().as_str(), m2.as_str());
        } else if let Some(m1) = cap.get(1) {
             // ref('table') -> table
             return m1.as_str().to_string();
        }
    }
    
    // Fallback: cleaning up potential jinja braces or quotes for simple identifiers
    let cleaned = source.replace("{{", "").replace("}}", "").trim().to_string();
    cleaned.trim_matches(|c| c == '"' || c == '`').to_string()
}
