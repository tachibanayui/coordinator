mod coordinator;

/// An attribute macro that automatically generate enum dispatch and other boilerplate code for [`TaskProcessor`].
/// It is recommended to use this macro over writing your own enum dispatch if you have a [`TaskProcessor`] that
/// can process more than 1 type of task (Eg: a [`CalculatorProcessor`] can handle both plus and mult operations)
/// Please see the `coordinator` crate documentation for examples.
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
