use tree_sitter::{Parser, Tree};
use tree_sitter_sql_bigquery;

pub struct DbtParser {
    parser: Parser,
}

impl DbtParser {
    pub fn new() -> anyhow::Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_sql_bigquery::language();
        parser.set_language(&language)?;
        Ok(Self { parser })
    }

    pub fn parse(&mut self, text: &str, old_tree: Option<&Tree>) -> Option<Tree> {
        self.parser.parse(text, old_tree)
    }
}
