use anyhow::Result;
use arrow_array::{
    cast::AsArray,
    types::{Int32Type, Int64Type},
    Array, Int32Array, Int64Array, RecordBatchReader,
};
use arrow_schema::{DataType, Field};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{ffi::OsString, fs::File};
use tabled::{builder::Builder, settings::Style, Table, Tabled};

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

trait ToStringArray {
    fn get_len(&self) -> usize;
    fn get_string(&self, i: usize) -> String;
}

macro_rules! impl_to_string_array {
    ($($t: ty),* $(,)?) => {
        $(
            impl ToStringArray for $t {
                fn get_len(&self) -> usize {
                    self.len()
                }

                fn get_string(&self, i: usize) -> String {
                    self.value(i).to_string()
                }
            }
        )*
    };
}

impl_to_string_array!(Int32Array, Int64Array);

macro_rules! case_as_primitive {
    ($arr: expr, $($id: ident, $t: ty),* $(,)?) => {
        match $arr.data_type() {
            $(arrow_schema::DataType::$id => $arr.as_primitive::<$t>(),)*
            _ => unimplemented!("Unsupported data type"),
        }
    };
}

fn transform_array(arr: &dyn Array) -> &dyn ToStringArray {
    case_as_primitive!(arr, Int32, Int32Type, Int64, Int64Type)
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
    for batch in reader {
        let batch = batch?;
        let field_names = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let columns = batch
            .columns()
            .iter()
            .map(|c| transform_array(c.as_ref()))
            .collect::<Vec<_>>();
        if let Some(col) = columns.get(0) {
            let len = col.get_len();
            let mut builder = Builder::default();
            for i in 0..len {
                let row = columns
                    .iter()
                    .map(|col| col.get_string(i))
                    .collect::<Vec<_>>();
                builder.push_record(row);
            }
            builder.set_header(field_names);
            println!("{}", builder.build().with(Style::rounded()));
        }
    }
    Ok(())
}
