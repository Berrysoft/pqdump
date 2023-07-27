use anyhow::Result;
use arrow_array::{cast::AsArray, types::*, *};
use arrow_schema::{DataType, Field};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{collections::VecDeque, ffi::OsString, fs::File};
use tabled::{builder::Builder, settings::Style, Table, Tabled};
use tryiterator::TryIteratorExt;

#[derive(Debug, Parser)]
#[command(about, version, author)]
struct Options {
    #[arg()]
    input: OsString,
    #[arg(short = 'n', long)]
    /// Print the number of rows and exit.
    length: bool,
    #[arg(short = 'A', long)]
    /// Print the datatypes only.
    only_types: bool,
    #[arg(long)]
    /// Suppress printing the datatypes.
    no_types: bool,
    #[command(flatten)]
    slice: SliceOptions,
}

#[derive(Debug, Parser)]
#[group(multiple = false)]
struct SliceOptions {
    #[arg(long)]
    head: Option<usize>,
    #[arg(long)]
    tail: Option<usize>,
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
    Float16Array,
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
    (Float16Array, Float16Type),
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
        Float16Type,
        Float32Type,
        Float64Type,
        Int8Type,
        Int16Type,
        Int32Type,
        Int64Type,
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
    )
}

fn main() -> Result<()> {
    let args = Options::parse();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(args.input)?)?.build()?;
    if args.length {
        let len = reader
            .into_iter()
            .map(|batch| batch.map(|batch| batch.num_rows()))
            .try_fold(0, |sum, i| i.map(|i| sum + i))?;
        println!("{}", len);
    } else {
        if !args.no_types {
            let fields = reader
                .schema()
                .fields()
                .iter()
                .map(|f| PrintedField::from(f.as_ref()))
                .collect::<Vec<_>>();
            println!("{}", Table::new(fields).with(Style::rounded()));
        }
        if !args.only_types {
            let field_names = reader
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();
            let rows = reader
                .into_iter()
                .map(|batch| {
                    batch.map(|batch| {
                        let columns = batch
                            .columns()
                            .iter()
                            .map(|c| transform_array(c.as_ref()))
                            .collect::<Vec<_>>();
                        (0..batch.num_rows())
                            .map(|i| {
                                anyhow::Ok(
                                    columns
                                        .iter()
                                        .map(|col| col.get_string(i))
                                        .collect::<Vec<_>>(),
                                )
                            })
                            .collect::<Vec<_>>()
                            .into_iter()
                    })
                })
                .try_flatten();
            if let Some(head) = args.slice.head {
                print_contents(field_names, rows.take(head))?;
            } else if let Some(tail) = args.slice.tail {
                let mut buf = VecDeque::new();
                for row in rows {
                    buf.push_back(row);
                    if buf.len() > tail {
                        buf.pop_front();
                    }
                }
                print_contents(field_names, buf.into_iter())?;
            } else {
                print_contents(field_names, rows)?;
            }
        }
    }
    Ok(())
}

fn print_contents<E>(
    columns: Vec<String>,
    rows: impl Iterator<Item = Result<Vec<String>, E>>,
) -> Result<(), E> {
    let mut builder = Builder::new();
    for row in rows {
        let row = row?;
        builder.push_record(row);
    }
    builder.set_header(columns);
    println!("{}", builder.build().with(Style::rounded()));
    Ok(())
}
