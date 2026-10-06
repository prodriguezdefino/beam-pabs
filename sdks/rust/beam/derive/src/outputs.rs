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

//! `#[derive(Outputs)]`.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Ident, Type};

use crate::attrs::{ContainerOpts, parse_id, reject_duplicates};

/// One output: a variant of the enum.
struct Output {
    variant: Ident,
    field: Ident,
    tag: String,
    ty: Type,
}

impl Output {
    fn parse(variant: &syn::Variant) -> syn::Result<Self> {
        let ty = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => fields.unnamed[0].ty.clone(),
            _ => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "an output variant has one field: the element type of the output",
                ));
            }
        };
        let field = format_ident!("{}", snake_case(&variant.ident.to_string()));
        let tag =
            parse_id(&variant.attrs, "output", |_| false)?.unwrap_or_else(|| field.to_string());
        Ok(Self {
            variant: variant.ident.clone(),
            field,
            tag,
            ty,
        })
    }
}

/// `EvenNumbers` becomes `even_numbers`.
fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Expands `#[derive(Outputs)]`: one output per variant, and a `<Enum>PCollections` struct
/// with one `PCollection` field per output.
pub(crate) fn expand_outputs(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    if opts.id.is_some() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "Outputs has no id; set the tag of an output with #[output(id = \"...\")]",
        ));
    }
    let krate = &opts.krate;
    let name = &input.ident;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            name,
            "Outputs needs an enum with one variant per output",
        ));
    };
    let outputs = data
        .variants
        .iter()
        .map(Output::parse)
        .collect::<syn::Result<Vec<_>>>()?;
    if outputs.is_empty() {
        return Err(syn::Error::new_spanned(
            name,
            "Outputs needs at least one variant",
        ));
    }
    reject_duplicates(outputs.iter().map(|o| (&o.tag, &o.variant)), "output")?;

    let vis = &input.vis;
    let collections = format_ident!("{name}PCollections");
    let mut generics = input.generics.clone();
    {
        let where_clause = generics.make_where_clause();
        for o in &outputs {
            let ty = &o.ty;
            where_clause
                .predicates
                .push(syn::parse_quote!(#ty: #krate::coders::DefaultCoder));
        }
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let docs = outputs.iter().map(|o| format!("The `{}` output.", o.tag));
    let fields = outputs.iter().map(|o| &o.field);
    let types = outputs.iter().map(|o| &o.ty);
    let declares = outputs.iter().map(|o| {
        let (field, tag) = (&o.field, &o.tag);
        quote!(#field: outputs.declare(#tag))
    });
    let arms = outputs.iter().map(|o| {
        let (variant, tag) = (&o.variant, &o.tag);
        quote!(Self::#variant(value) => emitter.push(::core::option::Option::Some(#tag), value))
    });
    let first = &outputs[0].field;
    let struct_doc = format!("The outputs of a transform that emits [`{name}`].");
    Ok(quote! {
        #[doc = #struct_doc]
        #vis struct #collections #impl_generics #where_clause {
            #(
                #[doc = #docs]
                pub #fields: #krate::values::PCollection<#types>,
            )*
        }

        impl #impl_generics #krate::values::POutput for #collections #ty_generics #where_clause {
            fn pipeline(&self) -> &#krate::pipeline::Pipeline {
                #krate::values::POutput::pipeline(&self.#first)
            }
        }

        impl #impl_generics #krate::transforms::Outputs for #name #ty_generics #where_clause {
            type Collections = #collections #ty_generics;

            fn declare(
                outputs: &mut #krate::internals::OutputDeclarer<'_>,
            ) -> Self::Collections {
                #collections { #(#declares),* }
            }

            fn emit_to(
                self,
                emitter: #krate::internals::Emitter<'_>,
            ) -> ::core::result::Result<(), ::std::string::String> {
                match self { #(#arms),* }
            }
        }
    })
}
