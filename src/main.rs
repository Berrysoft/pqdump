use anyhow::Result;
use arrow_array::{cast::AsArray, types::*, *};
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
    #[arg(short = 'A', long)]
    /// Print the datatypes.
    onlyattr: bool,
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

trait ToStringArray: Array {
    fn get_string(&self, i: usize) -> String;
}

macro_rules! impl_to_string_array {
    ($($t: ty),* $(,)?) => {
        $(
            impl ToStringArray for $t {
                fn get_string(&self, i: usize) -> String {
                    self.value(i).to_string()
                }
            }
        )*
    };
}

impl_to_string_array!(
    BooleanArray,
    Float32Array,
    Float64Array,
    Int8Array,
    Int16Array,
    Int32Array,
    Int64Array,
    UInt8Array,
    UInt16Array,
    UInt32Array,
    UInt64Array,
);

trait FromDynArray {
    type ArrayType: Array;

    fn from_dyn_array(arr: &dyn Array) -> &Self::ArrayType;
}

macro_rules! impl_from_array_primitive {
    ($(($arrty: ty, $t: ty)),* $(,)?) => {
        $(
            impl FromDynArray for $t {
                type ArrayType = $arrty;

                fn from_dyn_array(arr: &dyn Array) -> &Self::ArrayType {
                    arr.as_primitive::<$t>()
                }
            }
        )*
    };
}

impl_from_array_primitive!(
    (Float32Array, Float32Type),
    (Float64Array, Float64Type),
    (Int8Array, Int8Type),
    (Int16Array, Int16Type),
    (Int32Array, Int32Type),
    (Int64Array, Int64Type),
    (UInt8Array, UInt8Type),
    (UInt16Array, UInt16Type),
    (UInt32Array, UInt32Type),
    (UInt64Array, UInt64Type),
);

impl FromDynArray for BooleanType {
    type ArrayType = BooleanArray;

    fn from_dyn_array(arr: &dyn Array) -> &Self::ArrayType {
        arr.as_boolean()
    }
}

macro_rules! case_as_primitive {
    ($arr: expr, $($t: ty),* $(,)?) => {
        match $arr.data_type() {
            $(&<$t>::DATA_TYPE => <$t>::from_dyn_array($arr),)*
            _ => unimplemented!("Unsupported data type"),
        }
    };
}

fn transform_array(arr: &dyn Array) -> &dyn ToStringArray {
    case_as_primitive!(
        arr,
        BooleanType,
        Float32Type,
        Float64Type,
        Int8Type,
        Int16Type,
        Int32Type,
        Int64Type,
    )
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
    if !args.onlyattr {
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
                let len = col.len();
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
    }
    Ok(())
}
