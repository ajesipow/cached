use cached::Request;
use nom::branch::alt;
use nom::bytes::complete::{tag_no_case, take_until1};
use nom::character::complete::space1;
use nom::combinator::{map, verify};
use nom::sequence::{separated_pair, tuple};
use nom::IResult;

pub(crate) fn parse_input(input: &str) -> IResult<&str, Option<Request>> {
    alt((parse_flush, parse_get, parse_delete, parse_set, parse_exit))(input)
}

fn parse_flush(input: &str) -> IResult<&str, Option<Request>> {
    map(tag_no_case("flush"), |_| Some(Request::Flush))(input)
}

fn parse_exit(input: &str) -> IResult<&str, Option<Request>> {
    map(tag_no_case("exit"), |_| None)(input)
}

fn parse_get(input: &str) -> IResult<&str, Option<Request>> {
    map(
        separated_pair(
            tag_no_case("get"),
            space1,
            parse_str_until_newline_without_whitespaces,
        ),
        |(_, key): (&str, &str)| Some(Request::Get(key.to_string())),
    )(input)
}

fn parse_delete(input: &str) -> IResult<&str, Option<Request>> {
    map(
        separated_pair(
            tag_no_case("delete"),
            space1,
            parse_str_until_newline_without_whitespaces,
        ),
        |(_, key): (&str, &str)| Some(Request::Delete(key.to_string())),
    )(input)
}

fn parse_set(input: &str) -> IResult<&str, Option<Request>> {
    map(
        tuple((
            tag_no_case("set"),
            space1,
            take_until1(" "),
            space1,
            parse_str_until_newline_without_whitespaces,
        )),
        |(_, _, key, _, value): (&str, &str, &str, &str, &str)| {
            Some(Request::Set {
                key: key.to_string(),
                value: value.to_string(),
                ttl_since_unix_epoch_in_millis: None,
            })
        },
    )(input)
}

fn parse_str_until_newline_without_whitespaces(input: &str) -> IResult<&str, &str> {
    let parse_until_newline = take_until1("\n");
    verify(parse_until_newline, |s: &str| !s.contains(' '))(input)
}
