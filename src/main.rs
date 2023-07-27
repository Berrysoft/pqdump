use anyhow::Result;
use arrow_array::RecordBatchReader;
use arrow_schema::{DataType, Field};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{ffi::OsString, fs::File};
use tabled::{settings::Style, Table, Tabled};

#[derive(Debug, Parser)]
#[command(about, version, author)]
struct Options {
    #[arg()]
    input: OsString,
}

#[derive(Debug, Tabled)]
struct PrintedField {
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl From<&Field> for PrintedField {
    fn from(value: &Field) -> Self {
        Self {
            name: value.name().clone(),
            data_type: value.data_type().clone(),
            nullable: value.is_nullable(),
        }
    }
}

fn main() -> Result<()> {
    let args = Options::parse();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(args.input)?)?.build()?;
    let fields = reader
        .schema()
        .fields()
        .iter()
        .map(|f| PrintedField::from(f.as_ref()))
        .collect::<Vec<_>>();
    println!("{}", Table::new(fields).with(Style::rounded()));
    Ok(())
}
