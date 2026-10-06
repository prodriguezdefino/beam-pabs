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

use std::collections::HashSet;

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Fields, GenericArgument, GenericParam, Generics, Ident, LitInt, LitStr,
    Path, PathArguments, Type,
};

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
    ty: Type,
    is_bytes: bool,
    beam_name: String,
    field_type: TokenStream2,
    matches_field_type: TokenStream2,
    nullable_const: TokenStream2,
    is_null: TokenStream2,
    to_value: TokenStream2,
    encode_wire: TokenStream2,
    decode_wire: TokenStream2,
    decode_null: TokenStream2,
    encoding_position: Option<i32>,
}

impl SchemaField {
    fn from_syn(
        field: &syn::Field,
        ident: Ident,
        opts: FieldOpts,
        krate: &Path,
    ) -> syn::Result<Self> {
        let ty = field.ty.clone();
        if opts.bytes {
            reject_non_vec_u8(&ty)?;
        }

        let beam_name = opts.rename.unwrap_or_else(|| ident.to_string());
        let (
            field_type,
            matches_field_type,
            nullable_const,
            is_null,
            to_value,
            encode_wire,
            decode_wire,
            decode_null,
        ) = if opts.bytes {
            (
                quote!(#krate::schema::derive_support::bytes_field_type()),
                quote!(#krate::schema::derive_support::bytes_matches_field_type),
                quote!(false),
                quote!(false),
                quote!(#krate::schema::derive_support::bytes_to_field_value(&self.#ident)?),
                quote!(#krate::schema::derive_support::bytes_encode_field(&self.#ident, writer)?),
                quote!(#krate::schema::derive_support::bytes_decode_field(reader)?),
                quote!(#krate::schema::derive_support::bytes_decode_null()?),
            )
        } else {
            (
                quote!(<#ty as #krate::schema::BeamField>::beam_field_type()),
                quote!(<#ty as #krate::schema::BeamField>::matches_field_type),
                quote!(<#ty as #krate::schema::BeamField>::NULLABLE),
                quote!(<#ty as #krate::schema::BeamField>::is_null(&self.#ident)),
                quote!(<#ty as #krate::schema::BeamField>::to_field_value(&self.#ident)?),
                quote!(<#ty as #krate::schema::BeamField>::encode_field(&self.#ident, writer)?),
                quote!(<#ty as #krate::schema::BeamField>::decode_field(reader)?),
                quote!(<#ty as #krate::schema::BeamField>::decode_null()?),
            )
        };

        Ok(Self {
            ident,
            ty,
            is_bytes: opts.bytes,
            beam_name,
            field_type,
            matches_field_type,
            nullable_const,
            is_null,
            to_value,
            encode_wire,
            decode_wire,
            decode_null,
            encoding_position: opts.encoding_position,
        })
    }

    fn decode_slot(&self, slot_expr: &TokenStream2, krate: &Path) -> TokenStream2 {
        if self.is_bytes {
            quote!(#krate::schema::derive_support::bytes_from_field_value(#slot_expr)?)
        } else {
            let ty = &self.ty;
            quote!(<#ty as #krate::schema::BeamField>::from_field_value(#slot_expr)?)
        }
    }
}

/// A struct field annotated `#[beam(skip)]`.
struct SkippedField {
    ident: Ident,
    ty: Type,
}

/// Splits struct fields into active schema fields and `#[beam(skip)]` fields.
fn partition_fields(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::Token![,]>,
    krate: &Path,
) -> syn::Result<(Vec<SchemaField>, Vec<SkippedField>)> {
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
        .map(|(field, ident, _)| SkippedField {
            ident,
            ty: field.ty.clone(),
        })
        .collect();

    let schema_fields = active_entries
        .into_iter()
        .map(|(field, ident, opts)| SchemaField::from_syn(field, ident, opts, krate))
        .collect::<syn::Result<Vec<_>>>()?;

    Ok((schema_fields, skipped))
}

pub(crate) fn expand_row(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    let krate = &opts.krate;
    let name = &input.ident;
    let type_params = collect_type_params(&input.generics)?;
    let has_type_params = !type_params.is_empty();

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
    reject_duplicate_names(&schema_fields)?;
    check_encoding_positions(&schema_fields)?;

    let (row_generics, coder_generics) = build_impl_generics(
        &input.generics,
        &type_params,
        &schema_fields,
        &skipped,
        krate,
    );
    let (row_impl, ty_generics, row_where) = row_generics.split_for_impl();
    let (nn_impl, _, nn_where) = input.generics.split_for_impl();
    let (coder_impl, _, coder_where) = coder_generics.split_for_impl();

    let id = match &opts.id {
        Some(id) => quote!(::core::option::Option::Some(#id.to_string())),
        None => quote!(::core::option::Option::None),
    };

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

    let build_schema = quote! {
        let mut schema = #krate::schema::Schema::new(::std::vec![
            #(#field_decls),*
        ]);
        schema.id = #id;
        ::std::sync::Arc::new(schema)
    };
    let beam_schema_body = if has_type_params {
        build_schema
    } else {
        quote! {
            static SCHEMA: ::std::sync::OnceLock<
                ::std::sync::Arc<#krate::schema::Schema>
            > = ::std::sync::OnceLock::new();

            ::std::sync::Arc::clone(SCHEMA.get_or_init(|| {
                #build_schema
            }))
        }
    };

    let num_fields = schema_fields.len();
    let positions_set = schema_fields.iter().any(|f| f.encoding_position.is_some());
    let wire_order = compute_wire_order(&schema_fields);

    let to_values = schema_fields.iter().map(|f| &f.to_value);
    let skipped_inits: Vec<_> = skipped
        .iter()
        .map(|s| {
            let ident = &s.ident;
            quote!(#ident: ::core::default::Default::default())
        })
        .collect();

    let matches_checks = schema_fields.iter().enumerate().map(|(index, f)| {
        let beam_name = &f.beam_name;
        let matches_ft = &f.matches_field_type;
        let pos_check = f.encoding_position.map(|pos| {
            quote!(&& schema.fields[#index].encoding_position == ::core::option::Option::Some(#pos))
        });
        quote! {
            && schema.fields[#index].name == #beam_name
            #pos_check
            && #matches_ft(&schema.fields[#index].field_type)
        }
    });

    let same_name_checks = schema_fields.iter().enumerate().map(|(index, f)| {
        let beam_name = &f.beam_name;
        quote!(&& __fields[#index].name == #beam_name)
    });

    let support = quote!(#krate::schema::derive_support);
    let positional_inits = schema_fields.iter().enumerate().map(|(index, f)| {
        let ident = &f.ident;
        let val = f.decode_slot(&quote!(#support::row_slot(__values, #index)?), krate);
        quote!(#ident: #val)
    });
    // `writer_index` is the position of the field in the row, or `None` if the row lacks it.
    let mapped_init = |f: &SchemaField, writer_index: TokenStream2| {
        let ident = &f.ident;
        let beam_name = &f.beam_name;
        let nullable_const = &f.nullable_const;
        let slot = quote! {
            #support::mapped_slot(__values, #writer_index, #nullable_const, #beam_name)?
        };
        let val = f.decode_slot(&slot, krate);
        quote!(#ident: #val)
    };
    let by_name_inits = schema_fields.iter().map(|f| {
        let beam_name = &f.beam_name;
        mapped_init(f, quote!(row.schema().field_index(#beam_name)))
    });
    let mapped_inits = schema_fields
        .iter()
        .enumerate()
        .map(|(index, f)| mapped_init(f, quote!(indices.get(#index).copied().flatten())));

    let wire_null_flags = wire_order.iter().map(|&idx| &schema_fields[idx].is_null);
    let wire_encodes = wire_order.iter().enumerate().map(|(pos, &idx)| {
        let f = &schema_fields[idx];
        let encode_wire = &f.encode_wire;
        if f.is_bytes {
            quote!(#encode_wire;)
        } else {
            quote! {
                if !__nulls[#pos] {
                    #encode_wire;
                }
            }
        }
    });

    let wire_decodes = wire_order.iter().enumerate().map(|(pos, &idx)| {
        let f = &schema_fields[idx];
        let var = format_ident!("__field_{idx}");
        let decode_null = &f.decode_null;
        let decode_wire = &f.decode_wire;
        quote! {
            let #var = if #support::bit_is_null(__null_bits, #pos) {
                #decode_null
            } else {
                #decode_wire
            };
        }
    });
    let wire_struct_inits = schema_fields.iter().enumerate().map(|(idx, f)| {
        let ident = &f.ident;
        let var = format_ident!("__field_{idx}");
        quote!(#ident: #var)
    });

    Ok(quote! {
        impl #row_impl #krate::schema::BeamRow for #name #ty_generics #row_where {
            fn beam_schema() -> ::std::sync::Arc<#krate::schema::Schema> {
                #beam_schema_body
            }

            fn to_row(&self) -> ::core::result::Result<
                #krate::schema::Row,
                #krate::schema::SchemaError,
            > {
                let values = ::std::vec![#(#to_values),*];
                #krate::schema::Row::new(
                    <Self as #krate::schema::BeamRow>::beam_schema(),
                    values,
                )
            }

            fn from_row(row: &#krate::schema::Row) -> ::core::result::Result<
                Self,
                #krate::schema::SchemaError,
            > {
                let __fields = &row.schema().fields;
                let __values = row.values();
                if __fields.len() == #num_fields #(#same_name_checks)* {
                    ::core::result::Result::Ok(Self {
                        #(#positional_inits,)*
                        #(#skipped_inits,)*
                    })
                } else {
                    ::core::result::Result::Ok(Self {
                        #(#by_name_inits,)*
                        #(#skipped_inits,)*
                    })
                }
            }

            fn matches_schema(schema: &#krate::schema::Schema) -> bool {
                schema.fields.len() == #num_fields
                    && schema.encoding_positions_set == #positions_set
                    #(#matches_checks)*
            }

            fn from_row_mapped(
                row: &#krate::schema::Row,
                indices: &[::core::option::Option<usize>],
            ) -> ::core::result::Result<Self, #krate::schema::SchemaError> {
                let __values = row.values();
                ::core::result::Result::Ok(Self {
                    #(#mapped_inits,)*
                    #(#skipped_inits,)*
                })
            }

            fn encode_row_bytes(
                &self,
                writer: &mut ::std::vec::Vec<u8>,
            ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                let __nulls: [bool; #num_fields] = [#(#wire_null_flags),*];
                #support::write_row_header(#num_fields, |__position| __nulls[__position], writer);
                #(#wire_encodes)*
                ::core::result::Result::Ok(())
            }

            fn decode_row_bytes(
                reader: &mut &[u8],
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                let __null_bits = #support::read_row_header(#num_fields, reader)?;
                #(#wire_decodes)*
                ::core::result::Result::Ok(Self {
                    #(#wire_struct_inits,)*
                    #(#skipped_inits,)*
                })
            }
        }

        // Emitted per type because a blanket `impl<T: BeamRow> BeamField for T`
        // conflicts with primitive `BeamField` implementations.
        impl #row_impl #krate::schema::BeamField for #name #ty_generics #row_where {
            fn beam_field_type() -> #krate::schema::FieldType {
                #krate::schema::FieldType::row(<Self as #krate::schema::BeamRow>::beam_schema())
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

            fn decode_null() -> ::core::result::Result<Self, #krate::coders::CoderError> {
                ::core::result::Result::Err(#krate::coders::CoderError::Schema(
                    #krate::schema::SchemaError::UnexpectedNull {
                        expected: "ROW".to_string(),
                    },
                ))
            }

            fn encode_field(
                &self,
                writer: &mut ::std::vec::Vec<u8>,
            ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                <Self as #krate::schema::BeamRow>::encode_row_bytes(self, writer)
            }

            fn decode_field(
                reader: &mut &[u8],
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                <Self as #krate::schema::BeamRow>::decode_row_bytes(reader)
            }

            fn matches_type_info(type_info: &#krate::schema::TypeInfo) -> bool {
                ::core::matches!(
                    type_info,
                    #krate::schema::TypeInfo::Row(inner)
                        if <Self as #krate::schema::BeamRow>::matches_schema(inner)
                )
            }
        }

        impl #nn_impl #krate::schema::NotNullable for #name #ty_generics #nn_where {}

        impl #coder_impl #krate::coders::DefaultCoder for #name #ty_generics #coder_where {
            type Coder = #krate::coders::RowStructCoder<Self>;

            fn coder() -> Self::Coder {
                #krate::coders::RowStructCoder::new()
            }

            fn encode_element(
                &self,
                writer: &mut ::std::vec::Vec<u8>,
            ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                <Self as #krate::schema::BeamRow>::encode_row_bytes(self, writer)
            }

            fn decode_element(
                reader: &mut &[u8],
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                <Self as #krate::schema::BeamRow>::decode_row_bytes(reader)
            }

            fn decode_element_with_context(
                reader: &mut &[u8],
                schema: ::core::option::Option<&::std::sync::Arc<#krate::schema::Schema>>,
                field_map: ::core::option::Option<&#krate::coders::FieldMap>,
                _state_reader: ::core::option::Option<
                    &::std::sync::Arc<dyn #krate::coders::StateStreamReader>,
                >,
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                match (schema, field_map) {
                    (::core::option::Option::None, _)
                    | (_, ::core::option::Option::Some(#krate::coders::FieldMap::Same)) => {
                        <Self as #krate::schema::BeamRow>::decode_row_bytes(reader)
                    }
                    (::core::option::Option::Some(s), ::core::option::Option::None)
                        if <Self as #krate::schema::BeamRow>::matches_schema(s) =>
                    {
                        <Self as #krate::schema::BeamRow>::decode_row_bytes(reader)
                    }
                    (
                        ::core::option::Option::Some(s),
                        ::core::option::Option::Some(#krate::coders::FieldMap::ByName(indices)),
                    ) => {
                        let row = #krate::coders::RowCoder::decode_row(s, reader)?;
                        ::core::result::Result::Ok(
                            <Self as #krate::schema::BeamRow>::from_row_mapped(&row, indices)?
                        )
                    }
                    (::core::option::Option::Some(s), ::core::option::Option::None) => {
                        let row = #krate::coders::RowCoder::decode_row(s, reader)?;
                        ::core::result::Result::Ok(<Self as #krate::schema::BeamRow>::from_row(&row)?)
                    }
                }
            }

            fn resolve_field_map(
                schema: &#krate::schema::Schema,
            ) -> ::core::result::Result<
                ::core::option::Option<#krate::coders::FieldMap>,
                #krate::schema::SchemaError,
            > {
                <Self as #krate::schema::BeamRow>::field_map(schema)
                    .map(::core::option::Option::Some)
            }

            fn register_coder<__R: #krate::coders::CoderRegistry + ?Sized>(
                registry: &__R,
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

/// Collects type parameter identifiers and rejects lifetime or const generic parameters.
fn collect_type_params(generics: &Generics) -> syn::Result<Vec<Ident>> {
    generics
        .params
        .iter()
        .map(|param| match param {
            GenericParam::Type(tp) => Ok(tp.ident.clone()),
            GenericParam::Lifetime(lt) => Err(syn::Error::new_spanned(
                lt,
                "BeamRow does not support lifetime parameters; decoded rows own their values",
            )),
            GenericParam::Const(c) => Err(syn::Error::new_spanned(
                c,
                "BeamRow does not support const generic parameters",
            )),
        })
        .collect()
}

/// Builds the impl generics for `BeamRow`/`BeamField` and `DefaultCoder`.
fn build_impl_generics(
    generics: &Generics,
    type_params: &[Ident],
    schema_fields: &[SchemaField],
    skipped: &[SkippedField],
    krate: &Path,
) -> (Generics, Generics) {
    if type_params.is_empty() {
        return (generics.clone(), generics.clone());
    }
    let param_set: HashSet<&Ident> = type_params.iter().collect();
    let mut row_generics = generics.clone();
    {
        let where_clause = row_generics.make_where_clause();
        let mut seen_beam = HashSet::new();
        for f in schema_fields.iter().filter(|f| !f.is_bytes) {
            if mentions_type_param(&f.ty, &param_set) {
                let ty = &f.ty;
                if seen_beam.insert(quote!(#ty).to_string()) {
                    where_clause
                        .predicates
                        .push(syn::parse_quote!(#ty: #krate::schema::BeamField));
                }
            }
        }
        let mut seen_default = HashSet::new();
        for s in skipped {
            let ty = &s.ty;
            if mentions_type_param(ty, &param_set) && seen_default.insert(quote!(#ty).to_string()) {
                where_clause
                    .predicates
                    .push(syn::parse_quote!(#ty: ::core::default::Default));
            }
        }
    }

    let mut coder_generics = row_generics.clone();
    {
        let where_clause = coder_generics.make_where_clause();
        let mut seen_sync = HashSet::new();
        for ty in schema_fields
            .iter()
            .map(|f| &f.ty)
            .chain(skipped.iter().map(|s| &s.ty))
        {
            if mentions_type_param(ty, &param_set) && seen_sync.insert(quote!(#ty).to_string()) {
                where_clause.predicates.push(syn::parse_quote!(
                    #ty: ::core::marker::Send + ::core::marker::Sync + 'static
                ));
            }
        }
        for param in type_params {
            if !seen_sync.contains(&param.to_string()) {
                where_clause
                    .predicates
                    .push(syn::parse_quote!(#param: 'static));
            }
        }
    }

    (row_generics, coder_generics)
}

/// Returns true when `ty` references one of `params`.
fn mentions_type_param(ty: &Type, params: &HashSet<&Ident>) -> bool {
    match ty {
        Type::Path(tp) => {
            if let Some(qself) = &tp.qself
                && mentions_type_param(&qself.ty, params)
            {
                return true;
            }
            if tp.qself.is_none()
                && let Some(first) = tp.path.segments.first()
                && params.contains(&first.ident)
            {
                return true;
            }
            tp.path.segments.iter().any(|seg| match &seg.arguments {
                PathArguments::None => false,
                PathArguments::AngleBracketed(ab) => ab.args.iter().any(|arg| match arg {
                    GenericArgument::Type(inner) => mentions_type_param(inner, params),
                    GenericArgument::AssocType(assoc) => mentions_type_param(&assoc.ty, params),
                    _ => false,
                }),
                PathArguments::Parenthesized(p) => tokens_mention_param(quote!(#p), params),
            })
        }
        Type::Array(a) => mentions_type_param(&a.elem, params),
        Type::Group(g) => mentions_type_param(&g.elem, params),
        Type::Paren(p) => mentions_type_param(&p.elem, params),
        Type::Ptr(p) => mentions_type_param(&p.elem, params),
        Type::Reference(r) => mentions_type_param(&r.elem, params),
        Type::Slice(s) => mentions_type_param(&s.elem, params),
        Type::Tuple(t) => t.elems.iter().any(|elem| mentions_type_param(elem, params)),
        other => tokens_mention_param(quote!(#other), params),
    }
}

fn tokens_mention_param(tokens: TokenStream2, params: &HashSet<&Ident>) -> bool {
    tokens.into_iter().any(|tree| match tree {
        proc_macro2::TokenTree::Ident(ident) => params.contains(&ident),
        proc_macro2::TokenTree::Group(group) => tokens_mention_param(group.stream(), params),
        _ => false,
    })
}

/// Maps wire position `0..n` to declaration index `0..n`.
fn compute_wire_order(fields: &[SchemaField]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..fields.len()).collect();
    if fields.iter().any(|f| f.encoding_position.is_some()) {
        order.sort_by_key(|&idx| fields[idx].encoding_position.unwrap_or(0));
    }
    order
}

/// Rejects two schema fields with the same Beam name, also after `#[beam(rename)]`.
fn reject_duplicate_names(fields: &[SchemaField]) -> syn::Result<()> {
    let mut seen = HashSet::new();
    fields
        .iter()
        .find(|f| !seen.insert(f.beam_name.as_str()))
        .map_or(Ok(()), |f| {
            Err(syn::Error::new_spanned(
                &f.ident,
                format!(
                    "the Beam field name '{}' is used twice; \
                     give each field a unique #[beam(rename = \"...\")]",
                    f.beam_name
                ),
            ))
        })
}

/// Checks that `#[beam(encoding_position = N)]` is on every schema field or on none, and
/// that the positions are `0..n` in some order. The row coder reads the null bitmask and
/// the values in position order.
fn check_encoding_positions(fields: &[SchemaField]) -> syn::Result<()> {
    if fields.iter().all(|f| f.encoding_position.is_none()) {
        return Ok(());
    }
    let count = fields.len();
    let mut seen = vec![false; count];
    fields.iter().try_for_each(|f| {
        let error = |message: String| Err(syn::Error::new_spanned(&f.ident, message));
        let Some(position) = f.encoding_position else {
            return error(
                "set #[beam(encoding_position = N)] on every field, or on none; \
                 this field has no position"
                    .to_string(),
            );
        };
        match usize::try_from(position).ok().filter(|&p| p < count) {
            None => error(format!(
                "encoding_position {position} is out of range; \
                 use 0..{count}, one position per field"
            )),
            Some(index) if seen[index] => {
                error(format!("encoding_position {position} is used twice"))
            }
            Some(index) => {
                seen[index] = true;
                Ok(())
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
    let PathArguments::AngleBracketed(generics) = &vec.arguments else {
        return false;
    };
    generics.args.len() == 1
        && matches!(
            generics.args.first(),
            Some(GenericArgument::Type(element))
                if last_segment(element)
                    .is_some_and(|seg| seg.ident == "u8" && seg.arguments.is_none())
        )
}
