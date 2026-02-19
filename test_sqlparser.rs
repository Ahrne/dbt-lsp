use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;

fn main() {
    let sql = "
SELECT
 w.a,
 w.b,
wrong input here w.c,
 w.d
FROM test
";

    let dialect = BigQueryDialect {};
    let result = Parser::parse_sql(&dialect, sql);

    match result {
        Ok(ast) => {
            println!("Parsed successfully!");
            println!("{:#?}", ast);
        }
        Err(e) => {
            println!("Caught error: {}", e);
        }
    }
}
