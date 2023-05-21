use crate::dto::RustTypes;
use crate::ops::Operations;
use crate::rust;

use std::format as f;
use std::ops::Not;

use codegen_writer::g;
use codegen_writer::glines;
use heck::ToSnakeCase;
use heck::ToUpperCamelCase;

#[allow(clippy::too_many_lines)]
pub fn codegen(ops: &Operations, rust_types: &RustTypes) {
    glines![
        "//! Auto generated by `codegen/src/aws_conv.rs`", //
        "",
        "use super::*;",
        "",
    ];

    for (name, rust_type) in rust_types {
        match name.as_str() {
            "SelectObjectContentRequest" => continue,
            "SelectObjectContentInput" => continue,
            "LifecycleExpiration" => continue,
            _ => {}
        }

        match rust_type {
            rust::Type::Alias(_) => continue,
            rust::Type::Provided(_) => continue,
            rust::Type::Timestamp(_) => continue,
            rust::Type::List(_) => continue,
            rust::Type::Map(_) => continue,
            rust::Type::StrEnum(_) => {}
            rust::Type::Struct(_) => {}
            rust::Type::StructEnum(_) => {}
        }

        let s3s_path = f!("s3s::dto::{name}");
        let aws_path = aws_ty_path(name, ops, rust_types);

        g!("impl AwsConversion for {s3s_path} {{");
        g!("    type Target = {aws_path};");
        g!("type Error = S3Error;");
        g!();

        if contains_deprecated_field(name) {
            g!("#[allow(deprecated)]");
        }
        g!("fn try_from_aws(x: Self::Target) -> S3Result<Self> {{");
        match rust_type {
            rust::Type::Struct(ty) => {
                if ty.fields.is_empty() {
                    g!("let _ = x;");
                }

                g!("Ok(Self {{");
                for field in &ty.fields {
                    let s3s_field_name = field.name.as_str();
                    let aws_field_name = match s3s_field_name {
                        "checksum_crc32c" => "checksum_crc32_c",
                        "type_" => "r#type",
                        s => s,
                    };

                    if field.type_ == "SelectObjectContentEventStream" {
                        g!("{s3s_field_name}: Some(crate::event_stream::from_aws(x.{aws_field_name})),");
                        continue;
                    }

                    if field.type_ == "StreamingBlob" {
                        g!("{s3s_field_name}: Some(try_from_aws(x.{aws_field_name})?),");
                        continue;
                    }

                    let needs_unwrap = 'unwrap: {
                        if is_op_input(&ty.name, ops) && field.option_type.not() && field.is_required {
                            break 'unwrap true;
                        }
                        field.option_type.not() && field.default_value.is_none()
                    };

                    if needs_unwrap {
                        g!("{s3s_field_name}: unwrap_from_aws(x.{aws_field_name}, \"{s3s_field_name}\")?,");
                        continue;
                    }

                    // other cases
                    {
                        g!("{s3s_field_name}: try_from_aws(x.{aws_field_name})?,");
                    }
                }
                g!("}})");
            }
            rust::Type::StrEnum(ty) => {
                g!("Ok(match x {{");
                for variant in &ty.variants {
                    let s3s_variant_name = variant.name.as_str();
                    let aws_variant_name = match s3s_variant_name {
                        "CRC32C" => "Crc32C".to_owned(),
                        _ => s3s_variant_name.to_upper_camel_case(),
                    };
                    g!("{aws_path}::{aws_variant_name} => Self::from_static(Self::{s3s_variant_name}),");
                }
                g!("{aws_path}::Unknown(_) => Self::from(x.as_str().to_owned()),");
                g!("_ => Self::from(x.as_str().to_owned()),");
                g!("}})");
            }
            rust::Type::StructEnum(ty) => {
                g!("Ok(match x {{");
                for variant in &ty.variants {
                    g!("{aws_path}::{0}(v) => Self::{0}(try_from_aws(v)?),", variant.name);
                }
                g!("_ => unimplemented!(\"unknown variant of {aws_path}: {{x:?}}\"),");
                g!("}})");
            }
            _ => panic!(),
        }
        g!("}}");
        g!();

        if contains_deprecated_field(name) {
            g!("#[allow(deprecated)]");
        }
        g!("fn try_into_aws(x: Self) -> S3Result<Self::Target> {{");
        match rust_type {
            rust::Type::Struct(ty) if ty.name == "SelectObjectContentOutput" => {
                // TODO(blocking): SelectObjectContentOutput::try_into_aws
                g!("drop(x);");
                g!("unimplemented!(\"See https://github.com/Nugine/s3s/issues/5\")");
            }
            rust::Type::Struct(ty) => {
                if ty.fields.is_empty() {
                    g!("let _ = x;");
                    g!("let y = Self::Target::builder();");
                } else {
                    g!("let mut y = Self::Target::builder();");
                }

                for field in &ty.fields {
                    let s3s_field_name = field.name.as_str();
                    let aws_field_name = match s3s_field_name {
                        "checksum_crc32c" => "checksum_crc32_c",
                        "type_" => "type",
                        s => s,
                    };

                    if field.option_type {
                        g!("y = y.set_{aws_field_name}(try_into_aws(x.{s3s_field_name})?);");
                    } else {
                        g!("y = y.set_{aws_field_name}(Some(try_into_aws(x.{s3s_field_name})?));");
                    }
                }

                if is_op_input(&ty.name, ops) {
                    g!("y.build().map_err(S3Error::internal_error)");
                } else {
                    g!("Ok(y.build())");
                }
            }
            rust::Type::StrEnum(_) => {
                g!("Ok({aws_path}::from(x.as_str()))");
            }
            rust::Type::StructEnum(ty) => {
                g!("Ok(match x {{");
                for variant in &ty.variants {
                    g!("Self::{0}(v) => {aws_path}::{0}(try_into_aws(v)?),", variant.name);
                }
                g!("_ => unimplemented!(\"unknown variant of {}: {{x:?}}\"),", ty.name);
                g!("}})");
            }
            _ => panic!(),
        }
        g!("}}");

        g!("}}");
        g!();
    }
}

fn aws_ty_name(name: &str) -> &str {
    match name {
        "BucketCannedACL" => "BucketCannedAcl",
        "CORSConfiguration" => "CorsConfiguration",
        "CORSRule" => "CorsRule",
        "CSVInput" => "CsvInput",
        "CSVOutput" => "CsvOutput",
        "JSONInput" => "JsonInput",
        "JSONOutput" => "JsonOutput",
        "JSONType" => "JsonType",
        "MFADelete" => "MfaDelete",
        "MFADeleteStatus" => "MfaDeleteStatus",
        "ObjectCannedACL" => "ObjectCannedAcl",
        "SSEKMS" => "Ssekms",
        "SSES3" => "Sses3",
        "SelectObjectContentEvent" => "SelectObjectContentEventStream",
        _ => name,
    }
}

fn aws_ty_path(name: &str, ops: &Operations, rust_types: &RustTypes) -> String {
    let aws_name = aws_ty_name(name);

    for suffix in ["Input", "Output", "Error"] {
        if let Some(op_name) = name.strip_suffix(suffix) {
            if ops.contains_key(op_name) {
                return f!("aws_sdk_s3::operation::{}::{aws_name}", op_name.to_snake_case());
            }
        }
    }

    if let Some(rust::Type::Struct(ty)) = rust_types.get(name) {
        if ty.is_error_type {
            return f!("aws_sdk_s3::types::error::{aws_name}");
        }
    }

    f!("aws_sdk_s3::types::{aws_name}")
}

fn is_op_input(name: &str, ops: &Operations) -> bool {
    if let Some(op) = name.strip_suffix("Input") {
        if ops.contains_key(op) {
            return true;
        }
    }
    false
}

fn contains_deprecated_field(name: &str) -> bool {
    matches!(name, "LifecycleRule" | "ReplicationRule")
}
