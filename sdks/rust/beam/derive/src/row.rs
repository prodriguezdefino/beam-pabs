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

//! `#[derive(BeamRow)]`.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, LitInt, LitStr, Path, Type};

use crate::attrs::ContainerOpts;

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
    let parsed: Vec<_> = fields
        .iter()
        .map(|field| {
            let opts = FieldOpts::parse(field)?;
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| syn::Error::new_spanned(field, "expected a named field"))?;
            Ok((field, ident, opts))
        })
        .collect::<syn::Result<_>>()?;

    let (skipped_entries, active_entries): (Vec<_>, Vec<_>) =
        parsed.into_iter().partition(|(_, _, opts)| opts.skip);

    let skipped = skipped_entries
        .into_iter()
        .map(|(_, ident, _)| ident)
        .collect();

    let schema_fields = active_entries
        .into_iter()
        .enumerate()
        .map(|(index, (field, ident, opts))| {
            SchemaField::from_syn(field, ident, opts, index, krate)
        })
        .collect::<syn::Result<Vec<_>>>()?;

    Ok((schema_fields, skipped))
}

pub(crate) fn expand_row(input: &DeriveInput) -> syn::Result<TokenStream2> {
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

    let id = match &opts.id {
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
                    schema.id = #id;
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
                #krate::schema::FieldType::row(::std::sync::Arc::clone(
                    <Self as #krate::schema::BeamRow>::beam_schema()
                ))
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
