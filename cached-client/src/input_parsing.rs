use cached::{Key, Request, Value};
use nom::branch::alt;
use nom::bytes::complete::{tag_no_case, take_until1};
use nom::character::complete::space1;
use nom::combinator::{cut, map, map_res, verify};
use nom::error::{context, VerboseError, VerboseErrorKind};
use nom::sequence::{separated_pair, tuple};
use nom::IResult;

pub(crate) fn parse_input(input: &str) -> IResult<&str, Option<Request>, VerboseError<&str>> {
    alt((parse_set, parse_flush, parse_get, parse_delete, parse_exit))(input)
}

fn parse_flush(input: &str) -> IResult<&str, Option<Request>, VerboseError<&str>> {
    map(tag_no_case("flush"), |_| Some(Request::Flush))(input)
}

fn parse_exit(input: &str) -> IResult<&str, Option<Request>, VerboseError<&str>> {
    map(tag_no_case("exit"), |_| None)(input)
}

fn parse_get(input: &str) -> IResult<&str, Option<Request>, VerboseError<&str>> {
    map_res(
        separated_pair(
            tag_no_case("get"),
            context("Expected key", cut(space1)),
            context(
                "Expected key",
                cut(parse_str_until_newline_without_whitespaces),
            ),
        ),
        |(_, key): (&str, &str)| {
            let key = Key::parse(key.to_string())?;
            Ok::<_, cached::Error>(Some(Request::Get(key)))
        },
    )(input)
}

fn parse_delete(input: &str) -> IResult<&str, Option<Request>, VerboseError<&str>> {
    map_res(
        separated_pair(
            tag_no_case("delete"),
            context("Expected key", cut(space1)),
            parse_str_until_newline_without_whitespaces,
        ),
        |(_, key): (&str, &str)| {
            let key = Key::parse(key.to_string())?;
            Ok::<_, cached::Error>(Some(Request::Delete(key)))
        },
    )(input)
}

fn parse_set(input: &str) -> IResult<&str, Option<Request>, VerboseError<&str>> {
    map_res(
        tuple((
            tag_no_case("set"),
            context("Expected key", cut(space1)),
            context("Expected key", cut(take_until1(" "))),
            space1,
            context(
                "Expected value",
                cut(parse_str_until_newline_without_whitespaces),
            ),
        )),
        |(_, _, key, _, value): (&str, &str, &str, &str, &str)| {
            let key = Key::parse(key.to_string())?;
            let value = Value::parse(value.to_string())?;
            Ok::<_, cached::Error>(Some(Request::Set {
                key,
                value,
                ttl_since_unix_epoch_in_millis: None,
            }))
        },
    )(input)
}

fn parse_str_until_newline_without_whitespaces(
    input: &str,
) -> IResult<&str, &str, VerboseError<&str>> {
    let parse_until_newline = take_until1("\n");
    cut(verify(parse_until_newline, |s: &str| !s.contains(' ')))(input)
}

pub fn convert_error(err: VerboseError<&str>) -> Option<String> {
    for (_, e) in err.errors {
        if let VerboseErrorKind::Context(context) = e {
            return Some(context.to_owned());
        }
    }
    None
}
