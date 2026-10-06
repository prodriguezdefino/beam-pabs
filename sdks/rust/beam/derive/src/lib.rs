/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Derive macros for Apache Beam schemas.
//!
//! `apache-beam-core` re-exports these macros behind its `derive` feature. Depend on
//! `apache-beam` or `apache-beam-core`, not on this crate directly.
//!
//! Expanded code references Beam schema and coder types through `::beam` by default.
//! Internal crate tests override that path with `#[beam(crate = "crate")]`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, LitInt, LitStr, Path, Type, parse_macro_input};

/// Derives `BeamRow`, `BeamField`, and `DefaultCoder` for a struct with named fields.
///
/// # Container attributes
///
/// - `#[beam(id = "...")]` — sets the schema id.
/// - `#[beam(crate = "...")]` — path to Beam schema types; defaults to `::beam`.
///
/// # Field attributes
///
/// - `#[beam(rename = "...")]` — Beam field name; defaults to the Rust field name.
/// - `#[beam(skip)]` — omits the field from the schema and reconstructs it with [`Default`].
/// - `#[beam(bytes)]` — maps a `Vec<u8>` field to `BYTES` instead of an array.
/// - `#[beam(encoding_position = N)]` — sets the wire encoding position.
#[proc_macro_derive(BeamRow, attributes(beam))]
pub fn derive_beam_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_row(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives `BeamField` for a fieldless enum, mapping variant names to `STRING`.
///
/// Beam has no portable enum logical type, so variants encode as their identifier strings.
#[proc_macro_derive(BeamEnum, attributes(beam))]
pub fn derive_beam_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_enum(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Options parsed from container-level `#[beam(...)]` attributes.
struct ContainerOpts {
    schema_id: Option<String>,
    krate: Path,
}

impl ContainerOpts {
    fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let mut schema_id = None;
        let mut krate: Path = syn::parse_quote!(::beam);

        for attr in input.attrs.iter().filter(|a| a.path().is_ident("beam")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    schema_id = Some(meta.value()?.parse::<LitStr>()?.value());
                    Ok(())
                } else if meta.path.is_ident("crate") {
                    krate = meta.value()?.parse::<LitStr>()?.parse()?;
                    Ok(())
                } else {
                    Err(meta.error("unsupported beam container attribute"))
                }
            })?;
        }

        Ok(Self { schema_id, krate })
    }
}

/// Options parsed from field-level `#[beam(...)]` attributes.
#[derive(Default)]
struct FieldOpts {
    rename: Option<String>,
    skip: bool,
    bytes: bool,
    encoding_position: Option<i32>,
}

impl FieldOpts {
    fn parse(field: &syn::Field) -> syn::Result<Self> {
        let mut opts = Self::default();

        for attr in field.attrs.iter().filter(|a| a.path().is_ident("beam")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    opts.rename = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("skip") {
                    opts.skip = true;
                } else if meta.path.is_ident("bytes") {
                    opts.bytes = true;
                } else if meta.path.is_ident("encoding_position") {
                    opts.encoding_position = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
                } else {
                    return Err(meta.error("unsupported beam field attribute"));
                }
                Ok(())
            })?;
        }

        Ok(opts)
    }
}

/// A field that participates in the schema, paired with its generated expressions.
struct SchemaField {
    ident: Ident,
    beam_name: String,
    field_type: TokenStream2,
    to_value: TokenStream2,
    from_value: TokenStream2,
    encoding_position: Option<i32>,
}

impl SchemaField {
    fn from_syn(
        field: &syn::Field,
        ident: Ident,
        opts: FieldOpts,
        index: usize,
        krate: &Path,
    ) -> syn::Result<Self> {
        let ty = &field.ty;
        if opts.bytes {
            reject_non_vec_u8(ty)?;
        }

        let beam_name = opts.rename.unwrap_or_else(|| ident.to_string());
        let (field_type, to_value, from_value) = if opts.bytes {
            (
                quote!(#krate::schema::derive_support::bytes_field_type()),
                quote!(#krate::schema::derive_support::bytes_to_field_value(&self.#ident)?),
                quote!(#krate::schema::derive_support::bytes_from_field_value(
                    __slot(#index)?
                )?),
            )
        } else {
            (
                quote!(<#ty as #krate::schema::BeamField>::beam_field_type()),
                quote!(<#ty as #krate::schema::BeamField>::to_field_value(&self.#ident)?),
                quote!(<#ty as #krate::schema::BeamField>::from_field_value(__slot(#index)?)?),
            )
        };

        Ok(Self {
            ident,
            beam_name,
            field_type,
            to_value,
            from_value,
            encoding_position: opts.encoding_position,
        })
    }
}

/// Splits struct fields into active schema fields and `#[beam(skip)]` fields.
fn partition_fields(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::Token![,]>,
    krate: &Path,
) -> syn::Result<(Vec<SchemaField>, Vec<Ident>)> {
    let mut schema_fields = Vec::new();
    let mut skipped = Vec::new();

    for field in fields {
        let opts = FieldOpts::parse(field)?;
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new_spanned(field, "expected a named field"))?;

        if opts.skip {
            skipped.push(ident);
        } else {
            let index = schema_fields.len();
            schema_fields.push(SchemaField::from_syn(field, ident, opts, index, krate)?);
        }
    }

    Ok((schema_fields, skipped))
}

fn expand_row(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    let krate = &opts.krate;
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "BeamRow requires a struct with named fields; \
                     tuple and unit structs have no field names to map to a Beam schema",
                ));
            }
        },
        Data::Enum(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "BeamRow does not support enums; \
                 use #[derive(BeamEnum)] for a fieldless enum",
            ));
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "BeamRow does not support unions",
            ));
        }
    };

    let (schema_fields, skipped) = partition_fields(fields, krate)?;

    let schema_id = match &opts.schema_id {
        Some(id) => quote!(::core::option::Option::Some(#id.to_string())),
        None => quote!(::core::option::Option::None),
    };

    let positions_set = schema_fields.iter().any(|f| f.encoding_position.is_some());

    let field_decls = schema_fields.iter().map(|f| {
        let beam_name = &f.beam_name;
        let field_type = &f.field_type;
        match f.encoding_position {
            Some(pos) => quote! {
                #krate::schema::Field::new(#beam_name, #field_type)
                    .with_encoding_position(#pos)
            },
            None => quote!(#krate::schema::Field::new(#beam_name, #field_type)),
        }
    });

    let to_values = schema_fields.iter().map(|f| &f.to_value);
    let from_inits = schema_fields.iter().map(|f| {
        let ident = &f.ident;
        let from_value = &f.from_value;
        quote!(#ident: #from_value)
    });
    let skipped_inits = skipped
        .iter()
        .map(|ident| quote!(#ident: ::core::default::Default::default()));

    Ok(quote! {
        impl #krate::schema::BeamRow for #name {
            fn beam_schema() -> &'static ::std::sync::Arc<#krate::schema::Schema> {
                // One static cell per concrete type.
                static SCHEMA: ::std::sync::OnceLock<
                    ::std::sync::Arc<#krate::schema::Schema>
                > = ::std::sync::OnceLock::new();

                SCHEMA.get_or_init(|| {
                    let mut schema = #krate::schema::Schema::new(::std::vec![
                        #(#field_decls),*
                    ]);
                    schema.id = #schema_id;
                    schema.encoding_positions_set = #positions_set;
                    ::std::sync::Arc::new(schema)
                })
            }

            fn to_row(&self) -> ::core::result::Result<
                #krate::schema::Row,
                #krate::schema::SchemaError,
            > {
                let values = ::std::vec![#(#to_values),*];
                #krate::schema::Row::new(
                    ::std::sync::Arc::clone(<Self as #krate::schema::BeamRow>::beam_schema()),
                    values,
                )
            }

            fn from_row(row: &#krate::schema::Row) -> ::core::result::Result<
                Self,
                #krate::schema::SchemaError,
            > {
                let __values = row.values();
                let __slot = |index: usize| -> ::core::result::Result<
                    ::core::option::Option<&#krate::schema::FieldValue>,
                    #krate::schema::SchemaError,
                > {
                    __values
                        .get(index)
                        .map(::core::option::Option::as_ref)
                        .ok_or(#krate::schema::SchemaError::IndexOutOfBounds(
                            index,
                            __values.len(),
                        ))
                };

                ::core::result::Result::Ok(Self {
                    #(#from_inits,)*
                    #(#skipped_inits,)*
                })
            }
        }

        // Emitted per type because a blanket `impl<T: BeamRow> BeamField for T`
        // conflicts with primitive `BeamField` implementations.
        impl #krate::schema::BeamField for #name {
            fn beam_field_type() -> #krate::schema::FieldType {
                #krate::schema::FieldType::row(
                    (**<Self as #krate::schema::BeamRow>::beam_schema()).clone()
                )
            }

            fn to_field_value(&self) -> ::core::result::Result<
                ::core::option::Option<#krate::schema::FieldValue>,
                #krate::schema::SchemaError,
            > {
                <Self as #krate::schema::BeamRow>::to_row(self)
                    .map(|row| ::core::option::Option::Some(
                        #krate::schema::FieldValue::Row(row)
                    ))
            }

            fn from_field_value(
                value: ::core::option::Option<&#krate::schema::FieldValue>,
            ) -> ::core::result::Result<Self, #krate::schema::SchemaError> {
                match value {
                    ::core::option::Option::Some(
                        #krate::schema::FieldValue::Row(row)
                    ) => <Self as #krate::schema::BeamRow>::from_row(row),
                    ::core::option::Option::Some(other) => {
                        ::core::result::Result::Err(
                            #krate::schema::SchemaError::ValueTypeMismatch {
                                expected: "ROW".to_string(),
                                actual: ::std::format!("{other:?}"),
                            },
                        )
                    }
                    ::core::option::Option::None => ::core::result::Result::Err(
                        #krate::schema::SchemaError::UnexpectedNull {
                            expected: "ROW".to_string(),
                        },
                    ),
                }
            }
        }

        impl #krate::schema::NotNullable for #name {}

        impl #krate::coders::DefaultCoder for #name {
            type Coder = #krate::coders::RowStructCoder<Self>;

            fn coder() -> Self::Coder {
                #krate::coders::RowStructCoder::new()
            }

            fn encode_element(
                &self,
                writer: &mut dyn ::std::io::Write,
            ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                let row = <Self as #krate::schema::BeamRow>::to_row(self)?;
                #krate::coders::RowCoder::encode_row(&row, writer)
            }

            fn decode_element(
                reader: &mut dyn ::std::io::Read,
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                let row = #krate::coders::RowCoder::decode_row(
                    <Self as #krate::schema::BeamRow>::beam_schema(),
                    reader,
                )?;
                ::core::result::Result::Ok(<Self as #krate::schema::BeamRow>::from_row(&row)?)
            }

            fn decode_element_with_schema(
                reader: &mut dyn ::std::io::Read,
                schema: ::core::option::Option<&::std::sync::Arc<#krate::schema::Schema>>,
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                let s = schema.unwrap_or_else(|| <Self as #krate::schema::BeamRow>::beam_schema());
                let row = #krate::coders::RowCoder::decode_row(s, reader)?;
                ::core::result::Result::Ok(<Self as #krate::schema::BeamRow>::from_row(&row)?)
            }

            fn register_coder<R: #krate::coders::CoderRegistry + ?Sized>(
                registry: &R,
            ) -> ::std::string::String {
                let schema = <Self as #krate::schema::BeamRow>::beam_schema();
                registry.register_coder_with_payload(
                    #krate::coders::URN_ROW,
                    ::std::vec![],
                    schema.to_proto_bytes(),
                )
            }
        }
    })
}

fn expand_enum(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    let krate = &opts.krate;
    let name = &input.ident;

    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "BeamEnum only supports enums; use #[derive(BeamRow)] for structs",
        ));
    };

    // Beam has no portable tagged-union schema type, so variants cannot carry payloads.
    let variants = data
        .variants
        .iter()
        .map(|variant| match variant.fields {
            Fields::Unit => Ok(&variant.ident),
            _ => Err(syn::Error::new_spanned(
                variant,
                "BeamEnum supports only fieldless variants; \
                 Beam has no portable tagged-union type",
            )),
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let to_arms = variants.iter().map(|ident| {
        let label = ident.to_string();
        quote!(Self::#ident => #label)
    });
    let from_arms = variants.iter().map(|ident| {
        let label = ident.to_string();
        quote!(#label => ::core::result::Result::Ok(Self::#ident))
    });
    let expected = variants
        .iter()
        .map(|ident| ident.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    Ok(quote! {
        impl #krate::schema::BeamField for #name {
            fn beam_field_type() -> #krate::schema::FieldType {
                #krate::schema::FieldType::string()
            }

            fn to_field_value(&self) -> ::core::result::Result<
                ::core::option::Option<#krate::schema::FieldValue>,
                #krate::schema::SchemaError,
            > {
                let label = match self { #(#to_arms),* };
                ::core::result::Result::Ok(::core::option::Option::Some(
                    #krate::schema::FieldValue::String(label.to_string()),
                ))
            }

            fn from_field_value(
                value: ::core::option::Option<&#krate::schema::FieldValue>,
            ) -> ::core::result::Result<Self, #krate::schema::SchemaError> {
                match value {
                    ::core::option::Option::Some(
                        #krate::schema::FieldValue::String(label)
                    ) => match label.as_str() {
                        #(#from_arms,)*
                        other => ::core::result::Result::Err(
                            #krate::schema::SchemaError::ValueTypeMismatch {
                                expected: ::std::format!(
                                    "one of [{}]", #expected
                                ),
                                actual: other.to_string(),
                            },
                        ),
                    },
                    ::core::option::Option::Some(other) => ::core::result::Result::Err(
                        #krate::schema::SchemaError::ValueTypeMismatch {
                            expected: "STRING".to_string(),
                            actual: ::std::format!("{other:?}"),
                        },
                    ),
                    ::core::option::Option::None => ::core::result::Result::Err(
                        #krate::schema::SchemaError::UnexpectedNull {
                            expected: "STRING".to_string(),
                        },
                    ),
                }
            }
        }

        impl #krate::schema::NotNullable for #name {}
    })
}

/// Rejects `#[beam(bytes)]` when the field syntax is not `Vec<u8>`.
fn reject_non_vec_u8(ty: &Type) -> syn::Result<()> {
    if is_vec_u8(ty) {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            ty,
            "#[beam(bytes)] applies only to Vec<u8> fields; \
             use bytes::Bytes for a BYTES field of another type",
        ))
    }
}

/// Checks whether `ty` has the syntax `Vec<u8>` (with any path prefix).
fn is_vec_u8(ty: &Type) -> bool {
    fn last_segment(ty: &Type) -> Option<&syn::PathSegment> {
        match ty {
            Type::Path(path) if path.qself.is_none() => path.path.segments.last(),
            _ => None,
        }
    }

    let Some(vec) = last_segment(ty).filter(|segment| segment.ident == "Vec") else {
        return false;
    };
    let syn::PathArguments::AngleBracketed(generics) = &vec.arguments else {
        return false;
    };
    generics.args.len() == 1
        && matches!(
            generics.args.first(),
            Some(syn::GenericArgument::Type(element))
                if last_segment(element)
                    .is_some_and(|seg| seg.ident == "u8" && seg.arguments.is_none())
        )
}
