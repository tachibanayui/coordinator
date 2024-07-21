use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use quote::ToTokens;
use syn::parse_quote;
use syn::spanned::Spanned;
use syn::FnArg;
use syn::GenericParam;
use syn::Generics;
use syn::ItemTrait;
use syn::ReturnType;
use syn::TraitItem;
use syn::WherePredicate;

pub(crate) fn coordinator(
    _attr: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let body: ItemTrait = syn::parse2(item)?;
    let name = &body.ident;
    let name_enum_input = format_ident!("{}{}", name, "Input");
    let name_enum_output = format_ident!("{}{}", name, "Output");
    let name_any_ref = format_ident!("{}{}", name, "AnyRef");
    let name_prefer_ref = format_ident!("{}{}", name, "PreferRef");
    let name_require_ref = format_ident!("{}{}", name, "RequireRef");
    let name_trait = format_ident!("{}{}", name, "Processor");
    let name_trait_wrapper = format_ident!("{}{}", name, "ProcWrapper");

    let ident_generics = compose_generic(&body)?;
    let mut generics = ident_generics.clone();
    generics.params.push(parse_quote!(TIdent));
    let ident_bounds = parse_quote!(TIdent: ::std::hash::Hash + ::std::cmp::Eq + ::std::cmp::Ord + ::std::fmt::Debug + ::std::clone::Clone + ::std::marker::Send + ::std::marker::Sync + ::std::panic::UnwindSafe + 'static);
    generics.make_where_clause().predicates.push(ident_bounds);

    let in_enum = gen_io_enum(&body, &name_enum_input, &name_enum_output, &ident_generics)?;
    let ref_struct = gen_refs(
        &body,
        &name,
        &name_any_ref,
        &name_prefer_ref,
        &name_require_ref,
        &name_enum_input,
        &name_enum_output,
        &generics,
        &ident_generics,
    )?;
    let ms = gen_main_struct(
        &body,
        name,
        &name_any_ref,
        &name_prefer_ref,
        &name_require_ref,
        &name_enum_input,
        &name_enum_output,
        &name_trait,
        &name_trait_wrapper,
        &generics,
        &ident_generics,
    )?;

    let new_trait = gen_trait(
        &body,
        &name_trait,
        &name_trait_wrapper,
        &name_enum_input,
        &name_enum_output,
        &ident_generics,
    );

    Ok(quote! {
        #in_enum
        #new_trait
        #ref_struct
        #ms
    })
}

fn gen_trait(
    x: &ItemTrait,
    trait_name: &Ident,
    trait_name_wrapper: &Ident,
    in_name: &Ident,
    out_name: &Ident,
    ident_generics: &Generics,
) -> TokenStream {
    let mut new_trait = x.clone();
    new_trait.ident = trait_name.clone();
    let mut match_arms = vec![];
    for item in new_trait.items.iter_mut() {
        let TraitItem::Fn(item_fn) = item else {
            continue;
        };

        make_thread_safe(&mut item_fn.sig.generics);
        // Generate match arm
        let old_ident = &item_fn.sig.ident;
        let pascal_name = to_pascal_case(&old_ident.to_string());
        let pascal_ident = Ident::new(&pascal_name, old_ident.span());
        let params: Vec<_> = item_fn
            .sig
            .inputs
            .iter()
            .enumerate()
            .map(|(i, _)| format_ident!("p{}", i))
            .collect();

        let ma = quote! {
            #in_name :: #pascal_ident (#(#params),*) => {
                #out_name :: #pascal_ident (self.0.#old_ident(#(#params),*).await)
            }
        };

        match_arms.push(ma);

        // Convert to trait method
        item_fn.sig.inputs.insert(0, parse_quote!(&mut self));
        let old_ret = match &item_fn.sig.output {
            ReturnType::Default => quote!(()),
            ReturnType::Type(_, tok) => tok.to_token_stream(),
        };

        item_fn.sig.output =
            parse_quote!(-> impl ::std::future::Future<Output = #old_ret> + ::std::marker::Send );
    }

    let vis = &x.vis;
    let generic = &mut new_trait.generics;
    let mut mwc = generic.make_where_clause().clone();
    let iig = to_ident_generics(ident_generics);
    let igp = &ident_generics.params;
    ident_generics
        .where_clause
        .as_ref()
        .map(|x| mwc.predicates.extend(x.predicates.iter().cloned()));
    let ig = to_ident_generics(&generic);
    let wrapper = quote! {
        #vis struct #trait_name_wrapper<T>(T);
        impl<#igp, Slf: #trait_name #ig + ::std::marker::Send> ::coordinator::TaskProcessor <#in_name #iig> for #trait_name_wrapper <Slf> #mwc {
            type Output = #out_name #iig;

            fn do_work(
                &mut self,
                task: #in_name #iig,
            ) -> impl ::std::future::Future<Output = Self::Output> + Send {
                async {
                    let rs = match task {
                        #(#match_arms)*
                        _ => panic!("Logic error"),
                    };

                    rs
                }
            }
        }

    };

    return quote! {#new_trait #wrapper};
}

fn ident_generic_params(x: &GenericParam) -> &Ident {
    match &x {
        GenericParam::Lifetime(x) => &x.lifetime.ident,
        GenericParam::Const(x) => &x.ident,
        GenericParam::Type(x) => &x.ident,
    }
}

fn to_pascal_case(snake: &str) -> String {
    snake
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

fn gen_io_enum(
    body: &ItemTrait,
    in_name: &Ident,
    out_name: &Ident,
    generics: &Generics,
) -> Result<TokenStream, syn::Error> {
    let vis = &body.vis;
    let mut input_variant = vec![];
    let mut output_variant = vec![];

    for item in body.items.iter() {
        let TraitItem::Fn(item_fn) = item else {
            continue;
        };

        if let Some(x) = item_fn.sig.receiver() {
            return Err(syn::Error::new(
                x.span(),
                "#[coordinator] functions cannot use self parameter!",
            ));
        }

        let name = item_fn.sig.ident.to_string();
        let variant_name = to_pascal_case(&name);
        let variant_ident = Ident::new(&variant_name, item_fn.sig.ident.span());
        let inp = item_fn
            .sig
            .inputs
            .iter()
            .filter_map(|x| match x {
                FnArg::Receiver(_) => None,
                FnArg::Typed(x) => Some(x),
            })
            .map(|x| {
                let ty = &x.ty;
                let attrs = &x.attrs;
                quote! { #(#attrs)* #ty }
            });

        let inv = quote! (#variant_ident (#(#inp),*));
        input_variant.push(inv);

        let out = if let ReturnType::Type(_, x) = &item_fn.sig.output {
            x.to_token_stream()
        } else {
            quote! {()}
        };
        let outv = quote! (#variant_ident(#out));
        output_variant.push(outv);
    }

    let phantom = generics.params.iter().map(|x| match x {
        GenericParam::Type(x) => {
            let ident = &x.ident;
            quote! {#ident}
        }
        GenericParam::Lifetime(x) => {
            let lifetime = &x.lifetime;
            quote! {&#lifetime ()}
        }
        GenericParam::Const(x) => {
            let ident = &x.ident;
            quote! {#ident}
        }
    });

    let phantom_variant = quote! {PhantomVariant( ::std::marker::PhantomData<(#(#phantom),*)> )};
    let ig = to_ident_generics(generics);

    let debug_tps = generics
        .params
        .iter()
        .filter_map(|x| match x {
            GenericParam::Type(x) => Some(x),
            _ => None,
        })
        .map(|x| &x.ident)
        .map::<WherePredicate, _>(|x| parse_quote!(#x: ::std::fmt::Debug));

    let mut debug_wdc = ig
        .where_clause
        .clone()
        .unwrap_or_else(|| parse_quote!(where));

    debug_wdc.predicates.extend(debug_tps);

    Ok(quote! {
        #[derive(::std::fmt::Debug, ::std::clone::Clone)]
        #vis enum #in_name #ig {
            #(#input_variant),*,
            #phantom_variant
        }

        impl #generics ::core::fmt::Display for #in_name #ig #debug_wdc {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                ::std::fmt::Debug::fmt(&self, f)
            }
        }

        impl #generics ::std::error::Error for #in_name #ig #debug_wdc {}

        #[derive(::std::fmt::Debug, ::std::clone::Clone)]
        #vis enum #out_name #ig {
            #(#output_variant),*,
            #phantom_variant
        }
    })
}

fn gen_refs(
    body: &ItemTrait,
    name: &Ident,
    ref_name_any: &Ident,
    ref_name_preferred: &Ident,
    ref_name_required: &Ident,
    in_name: &Ident,
    out_name: &Ident,
    generics: &Generics,
    ident_generics: &Generics,
) -> Result<TokenStream, syn::Error> {
    let vis = &body.vis;
    let mut members_any = vec![];
    let mut members_prefer = vec![];
    let mut members_require = vec![];
    let iig = to_ident_generics(ident_generics);

    for item in body.items.iter() {
        let TraitItem::Fn(item_fn) = item else {
            continue;
        };

        let mut method = item_fn.clone();
        method.sig.asyncness = Some(parse_quote!(async));
        method.sig.inputs.insert(0, parse_quote!(&self));
        method.sig.generics.params.clear();
        method
            .sig
            .generics
            .where_clause
            .as_mut()
            .map(|x| x.predicates.clear());

        let old_ret = match &method.sig.output {
            ReturnType::Default => quote!(()),
            ReturnType::Type(_, ty) => ty.to_token_stream(),
        };
        let ret = parse_quote! {
            -> std::result::Result<
                impl ::coordinator::signal::JoinHandle<
                    Output = std::result::Result<
                        (#old_ret, TIdent),
                        ::coordinator::TaskProcessErr,
                    >,
                >,
                #in_name #iig,
            >
        };

        method.sig.output = ret;

        let pc = to_pascal_case(&item_fn.sig.ident.to_string());
        let pc_ident = Ident::new(&pc, item_fn.sig.ident.span());
        let inp: Vec<_> = item_fn
            .sig
            .inputs
            .iter()
            .filter_map(|x| match x {
                FnArg::Receiver(_) => None,
                FnArg::Typed(x) => Some(x),
            })
            .map(|x| {
                let pat = &x.pat;
                quote! {  #pat }
            })
            .collect();

        let body_builder = |prefs| {
            parse_quote! {
                {
                    use ::coordinator::JoinHandle;
                    let x = self.slf.inner.run(
                        #in_name:: #pc_ident (#(#inp),*),
                        #prefs
                    ).await?.map(|r| r.map(|(fo, ident)| {
                        if let #out_name::#pc_ident(x) = fo {
                            (x, ident)
                        } else {
                            panic!("Logic error!")
                        }
                    }));

                    return Ok(x)
                }
            }
        };

        method.default = Some(body_builder(quote! {::coordinator::TaskPrefs::Any}));
        members_any.push(quote!(#method));

        let mut method_prefer = method.clone();
        method_prefer.default = Some(body_builder(
            quote! {::coordinator::TaskPrefs::Preferred(self.ident.clone())},
        ));
        members_prefer.push(method_prefer);

        let mut method_require = method.clone();
        method_require.default = Some(body_builder(
            quote! {::coordinator::TaskPrefs::Required(self.ident.clone())},
        ));
        members_require.push(method_require);
    }

    let mut ref_generic = generics.clone();
    ref_generic.params.insert(0, parse_quote!('cg0));
    let irg = to_ident_generics(&ref_generic);
    let wc = &ref_generic.where_clause;

    let ig = to_ident_generics(generics);

    Ok(quote! {
        #vis struct #ref_name_any #ref_generic #wc {
            slf: &'cg0 #name #ig
        }

        impl #ref_generic #ref_name_any #irg #wc {
            #(#members_any)*
        }

        #vis struct #ref_name_preferred #ref_generic #wc {
            slf: &'cg0 #name #ig,
            ident: &'cg0 TIdent
        }

        impl #ref_generic #ref_name_preferred #irg #wc {
            #(#members_prefer)*
        }

        #vis struct #ref_name_required #ref_generic #wc {
            slf: &'cg0 #name #ig,
            ident: &'cg0 TIdent
        }

        impl #ref_generic #ref_name_required #irg #wc {
            #(#members_require)*
        }
    })
}

fn thread_safe_bounds() -> TokenStream {
    quote!(::std::marker::Send + ::std::marker::Sync + ::std::panic::UnwindSafe + 'static)
}

fn make_thread_safe(generics: &mut Generics) {
    let sb = thread_safe_bounds();
    generics.make_where_clause();
    for p in generics.params.iter() {
        if let GenericParam::Type(gty) = &p {
            let pid = &gty.ident;
            generics
                .where_clause
                .as_mut()
                .map(|x| x.predicates.push(parse_quote!(#pid: #sb)));
        }
    }
}

fn compose_generic(body: &ItemTrait) -> Result<Generics, syn::Error> {
    let sb = thread_safe_bounds();
    let mut generics = body.generics.clone();
    generics.make_where_clause();
    let is_ident_used = generics
        .params
        .iter()
        .find(|x| matches!(x, GenericParam::Type(ty) if ty.ident.to_string() == "TIdent"));

    if let Some(params) = is_ident_used {
        return Err(syn::Error::new(
            params.span(),
            "TIdent is a reserved type parameters!",
        ));
    }

    make_thread_safe(&mut generics);

    for item in body.items.iter() {
        let TraitItem::Fn(item_fn) = item else {
            continue;
        };

        for gi in item_fn.sig.generics.params.iter().cloned() {
            if generics
                .params
                .iter()
                .any(|pr| ident_generic_params(pr) == ident_generic_params(&gi))
            {
                return Err(syn::Error::new(gi.span(), "Type paramters must be unique for every function and impl block defined inside #[coordinator]!"));
            }

            if let GenericParam::Type(_) = &gi {
                let gi_ident = ident_generic_params(&gi);
                generics
                    .where_clause
                    .as_mut()
                    .unwrap()
                    .predicates
                    .push(parse_quote!(#gi_ident: #sb));
            }
            generics.params.push(gi);
        }
    }

    Ok(generics)
}

fn to_ident_generics(x: &Generics) -> Generics {
    let igp = x.params.iter().map(|x| match x {
        GenericParam::Lifetime(x) => x.to_token_stream(),
        GenericParam::Type(x) => x.ident.to_token_stream(),
        GenericParam::Const(x) => x.ident.to_token_stream(),
    });

    parse_quote! {<#(#igp),*>}
}

fn gen_main_struct(
    body: &ItemTrait,
    name: &Ident,
    any_ref_ident: &Ident,
    prefer_ref_ident: &Ident,
    require_ref_ident: &Ident,
    in_name: &Ident,
    out_name: &Ident,
    trait_name: &Ident,
    trait_wrapper_name: &Ident,
    generics: &Generics,
    ident_generics: &Generics,
) -> Result<TokenStream, syn::Error> {
    let vis = &body.vis;
    let wc = &generics.where_clause;
    let igp = to_ident_generics(ident_generics);

    let ig = to_ident_generics(generics);
    let mut ig_new_lt = ig.clone();
    ig_new_lt.params.insert(0, parse_quote!('cg0));

    let itg = to_ident_generics(&body.generics);

    Ok(quote! {
        #[derive(Debug)]
        #vis struct #name #generics #wc {
            pub inner: ::coordinator::Coordinator<#in_name #igp, #out_name #igp, TIdent>
        }

        impl #generics #name #ig #wc {
            pub async fn add_worker(
                &self,
                ident: TIdent,
                worker: impl #trait_name #itg + ::std::panic::UnwindSafe + ::std::marker::Send + ::std::marker::Sync + 'static
            ) -> bool {
                let wrapped = #trait_wrapper_name(worker);
                self.inner.add_worker(ident, wrapped).await
            }

            pub fn any<'cg0>(&'cg0 self) -> #any_ref_ident #ig_new_lt {
                #any_ref_ident {
                    slf: self
                }
            }

            pub fn prefer<'cg0>(&'cg0 self, ident: &'cg0 TIdent) -> #prefer_ref_ident #ig_new_lt {
                #prefer_ref_ident {
                    slf: self,
                    ident
                }
            }

            pub fn require<'cg0>(&'cg0 self, ident: &'cg0 TIdent) -> #require_ref_ident #ig_new_lt {
                #require_ref_ident {
                    slf: self,
                    ident
                }
            }
        }

        impl #generics ::core::convert::From<::coordinator::Coordinator<#in_name #igp, #out_name #igp, TIdent>> for #name #ig #wc {
            fn from(value: ::coordinator::Coordinator<#in_name #igp, #out_name #igp, TIdent>) -> Self {
                Self { inner: value }
            }
        }

        impl #generics ::std::clone::Clone for  #name #ig #wc {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone()
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::coordinator;
    use quote::quote;

    #[test]
    fn output() {
        let attr = quote! {#[balancer]};
        let body = quote! {
            pub trait MyBalancer<T: ::std::fmt::Debug> {
                fn wait(duration: Duration) -> Instant;
                fn ping() -> Duration;
                fn post_msg<M: ToString + Send>(msg: M);
                fn broadcast<BI: Display + Send>(item: BI);
                fn drain_broadcast<I: Display + RefUnwindSafe>(vec: Arc<Vec<I>>) -> usize
                where
                    Self: Send,
                {
                    async move {
                        let mut num = 0;
                        for i in vec.iter() {
                            self.broadcast(i).await;
                            num += 1;
                        }

                        return num;
                    }
                }
            }
        };

        let out = coordinator(attr, body).unwrap();
        println!("{}", out);
    }
}
