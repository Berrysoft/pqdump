use arrow_array::RecordBatchReader;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use clap::Parser;
use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL_CONDENSED, Cell, Table};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, errors::Result};
use std::{ffi::OsString, fs::File};

#[derive(Debug, Parser)]
#[command(about, version, author)]
struct Options {
    #[arg()]
    input: OsString,
    #[arg(short, long, default_value = "1024")]
    /// Batch size.
    batch: usize,
    #[command(flatten)]
    print: PrintOptions,
    #[command(flatten)]
    slice: SliceOptions,
    #[command(flatten)]
    col: ColOptions,
}

#[derive(Debug, Parser)]
#[group(multiple = false)]
struct PrintOptions {
    #[arg(short = 'n', long)]
    /// Print the number of rows and exit.
    length: bool,
    #[arg(long)]
    /// Print the number of row groups and exit.
    num_row_groups: bool,
    #[arg(short = 'A', long)]
    /// Print the datatypes only.
    only_types: bool,
    #[arg(long)]
    /// Suppress printing the datatypes.
    no_types: bool,
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

fn writeln<T: std::fmt::Display, W: std::io::Write>(out: &mut W, val: T) -> Result<()> {
    match std::writeln!(out, "{val}") {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn main() -> Result<()> {
    let args = Options::parse();
    let mut out = std::io::stdout();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(args.input)?)?
        .with_batch_size(args.batch);
    let metadata = reader.metadata();
    if args.print.num_row_groups {
        writeln(&mut out, metadata.num_row_groups())?;
        return Ok(());
    }
    let len = reader.metadata().file_metadata().num_rows() as usize;
    let reader = reader.build()?;
    if args.print.length {
        writeln(&mut out, len)?;
        return Ok(());
    }
    let schema = reader.schema();
    if !args.print.no_types {
        let fields = schema.fields().iter().map(|f| {
            vec![
                Cell::new(f.name()),
                Cell::new(f.data_type().to_string()),
                Cell::new(f.is_nullable().to_string()),
            ]
        });
        writeln(
            &mut out,
            Table::new()
                .load_preset(UTF8_FULL_CONDENSED)
                .apply_modifier(UTF8_ROUND_CORNERS)
                .set_header(vec!["name", "data type", "nullable"])
                .add_rows(fields),
        )?;
    }
    if !args.print.only_types {
        let field_names = schema.fields().iter().map(|f| f.name().clone());
        let (field_indices, field_names): (Vec<_>, Vec<_>) = if let Some(columns) = args.col.columns
        {
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
        let take_batches = (skip + take) / args.batch;
        let take_batches = if ((skip + take) % args.batch) != 0 {
            take_batches + 1
        } else {
            take_batches
        };
        let batches = reader
            .into_iter()
            .skip(skip_batches)
            .take(take_batches)
            .collect::<Result<Vec<_>, _>>()?;
        let columns = batches
            .iter()
            .map(|batch| {
                batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| field_indices.contains(i))
                    .map(|(_, c)| ArrayFormatter::try_new(c, &FormatOptions::default()))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|columns| (batch.num_rows(), columns))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = columns
            .iter()
            .flat_map(|(num_rows, columns)| {
                (0..*num_rows).map(|i| {
                    columns
                        .iter()
                        .map(move |col| col.value(i).try_to_string().map(Cell::new))
                })
            })
            .skip(skip)
            .take(take);
        let mut table = Table::new();
        for row in rows {
            table.add_row(row.collect::<Result<Vec<_>, _>>()?);
        }
        writeln(
            &mut out,
            table
                .set_header(field_names)
                .load_preset(UTF8_FULL_CONDENSED)
                .apply_modifier(UTF8_ROUND_CORNERS),
        )?;
    }
    Ok(())
}
