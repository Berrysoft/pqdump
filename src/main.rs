use anyhow::Result;
use arrow_array::RecordBatchReader;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{ffi::OsString, fs::File};
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
    #[arg(short, long, default_value = "1024")]
    /// Batch size.
    batch: usize,
    #[command(flatten)]
    slice: SliceOptions,
    #[command(flatten)]
    col: ColOptions,
}

#[derive(Debug, Parser)]
#[group(multiple = false)]
struct SliceOptions {
    #[arg(long)]
    /// Print the first rows.
    head: Option<usize>,
    #[arg(long)]
    /// Print the last rows.
    tail: Option<usize>,
}

#[derive(Debug, Parser)]
#[group(multiple = false)]
struct ColOptions {
    #[arg(long, value_delimiter = ',')]
    /// Print the specified columns.
    columns: Option<Vec<String>>,
    #[arg(long, value_delimiter = ',')]
    /// Suppress the specified columns.
    exclude: Option<Vec<String>>,
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
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(args.input)?)?
        .with_batch_size(args.batch);
    let len = reader.metadata().file_metadata().num_rows() as usize;
    let reader = reader.build()?;
    if args.length {
        println!("{}", len);
    } else {
        let schema = reader.schema();
        if !args.no_types {
            let fields = schema
                .fields()
                .iter()
                .map(|f| PrintedField::from(f.as_ref()))
                .collect::<Vec<_>>();
            println!("{}", Table::new(fields).with(Style::rounded()));
        }
        if !args.only_types {
            let field_names = schema.fields().iter().map(|f| f.name().clone());
            let (field_indices, field_names): (Vec<_>, Vec<_>) =
                if let Some(columns) = args.col.columns {
                    field_names
                        .enumerate()
                        .filter(|(_, n)| columns.contains(n))
                        .unzip()
                } else if let Some(exclude) = args.col.exclude {
                    field_names
                        .enumerate()
                        .filter(|(_, n)| !exclude.contains(n))
                        .unzip()
                } else {
                    field_names.enumerate().unzip()
                };
            let (skip, take) = if let Some(head) = args.slice.head {
                (0, head.min(len))
            } else if let Some(tail) = args.slice.tail {
                if len <= tail {
                    (0, len)
                } else {
                    (len - tail, tail)
                }
            } else {
                (0, len)
            };
            let skip_batches = skip / args.batch;
            let skip = skip % args.batch;
            let rows = reader
                .into_iter()
                .skip(skip_batches)
                .map(|batch| {
                    batch.map(|batch| {
                        let columns = batch
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| field_indices.contains(i))
                            .map(|(_, c)| ArrayFormatter::try_new(c, &FormatOptions::default()))
                            .try_collect::<Vec<_>>();
                        match columns {
                            Ok(columns) => (0..batch.num_rows())
                                .map(|i| {
                                    columns
                                        .iter()
                                        .map(|col| col.value(i).try_to_string())
                                        .try_collect::<Vec<_>>()
                                })
                                .collect::<Vec<_>>()
                                .into_iter(),
                            Err(e) => vec![Err(e)].into_iter(),
                        }
                    })
                })
                .try_flatten();
            print_contents(field_names, rows.skip(skip).take(take))?;
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
