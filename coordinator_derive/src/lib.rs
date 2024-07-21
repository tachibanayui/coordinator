mod coordinator;

#[proc_macro_attribute]
pub fn coordinator(
    _attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match coordinator::coordinator(_attr.into(), item.into()) {
        Ok(x) => x,
        Err(x) => x.to_compile_error(),
    }
    .into()
}
