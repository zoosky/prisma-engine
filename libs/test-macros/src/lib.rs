extern crate proc_macro;

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, Ident, ItemFn};

#[derive(Debug, FromMeta)]
struct TestOneConnectorArgs {
    /// The name of the connector to test.
    connector: String,
}

const CONNECTOR_NAMES: &[&'static str] = &["postgres", "mysql", "mysql_8", "sqlite"];

#[derive(Debug, FromMeta)]
struct TestEachConnectorArgs {
    /// Comma-separated list of connectors to exclude.
    #[darling(default)]
    ignore: Option<String>,
}

impl TestEachConnectorArgs {
    fn connectors_to_test(&self) -> impl Iterator<Item = &&str> {
        let ignore = self.ignore.as_ref().map(String::as_str);

        CONNECTOR_NAMES.iter().filter(move |connector_name| match ignore {
            Some(ignore) => !connector_name.starts_with(&ignore),
            None => true,
        })
    }
}

#[proc_macro_attribute]
pub fn test_each_connector(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attributes_meta: syn::AttributeArgs = parse_macro_input!(attr as AttributeArgs);
    let args = TestEachConnectorArgs::from_list(&attributes_meta);

    let test_function = parse_macro_input!(input as ItemFn);
    let async_test: bool = test_function.sig.asyncness.is_some();

    let tests = match (args, async_test) {
        (Ok(args), false) => test_each_connector_wrapper_functions(&args, &test_function),
        (Ok(args), true) => test_each_connector_async_wrapper_functions(&args, &test_function),
        (Err(err), _) => panic!("{}", err),
    };

    let output = quote! {
        #(#tests)*

        #test_function
    };

    output.into()
}

fn test_each_connector_wrapper_functions(
    args: &TestEachConnectorArgs,
    test_function: &ItemFn,
) -> Vec<proc_macro2::TokenStream> {
    let test_fn_name = &test_function.sig.ident;

    let mut tests = Vec::with_capacity(CONNECTOR_NAMES.len());

    for connector in args.connectors_to_test() {
        let connector_test_fn_name = Ident::new(&format!("{}_on_{}", test_fn_name, connector), Span::call_site());
        let connector_api_factory = Ident::new(&format!("{}_test_api", connector), Span::call_site());

        let test = quote! {
            #[test]
            fn #connector_test_fn_name() {
                let api = #connector_api_factory();

                #test_fn_name(&api)
            }
        };

        tests.push(test);
    }

    tests
}

fn test_each_connector_async_wrapper_functions(
    args: &TestEachConnectorArgs,
    test_function: &ItemFn,
) -> Vec<proc_macro2::TokenStream> {
    let test_fn_name = &test_function.sig.ident;

    let mut tests = Vec::with_capacity(CONNECTOR_NAMES.len());

    for connector in args.connectors_to_test() {
        let connector_test_fn_name = Ident::new(&format!("{}_on_{}", test_fn_name, connector), Span::call_site());
        let connector_api_factory = Ident::new(&format!("{}_test_api", connector), Span::call_site());

        let test = quote! {
            #[test]
            fn #connector_test_fn_name() {
                let api = #connector_api_factory();

                async_std::task::block_on(#test_fn_name(&api))
                // To be enabled when we asyncify the migration engine.
                // TEST_ASYNC_RUNTIME.block_on(#test_fn_name(&api))
            }
        };

        tests.push(test);
    }

    tests
}

#[proc_macro_attribute]
pub fn test_one_connector(attr: TokenStream, input: TokenStream) -> TokenStream {
    let attributes_meta: syn::AttributeArgs = parse_macro_input!(attr as AttributeArgs);
    let args = TestOneConnectorArgs::from_list(&attributes_meta).unwrap();

    let test_function = parse_macro_input!(input as ItemFn);

    let test_impl_name = &test_function.sig.ident;
    let test_fn_name = Ident::new(
        &format!("{}_on_{}", &test_function.sig.ident, args.connector),
        Span::call_site(),
    );
    let api_factory = Ident::new(&format!("{}_test_api", args.connector), Span::call_site());

    let output = quote! {
        #[test]
        fn #test_fn_name() {
            let api = #api_factory();

            #test_impl_name(&api)
        }

        #test_function
    };

    output.into()
}
