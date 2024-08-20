use proc_macro::TokenStream;
use quote::quote;

/// Implements the necessary functions to store a `T` in Mauve.
///
/// Requires: `Serialize + for<'de> Deserialize<'de>`
#[proc_macro_derive(MauveObject)]
pub fn mauve_object_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_mauve_object(&ast)
}

fn impl_mauve_object(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl ToFromMauve<#name> for #name {
            fn to_object(&self) -> Result<Vec<u8>, MauveError> {
                let mut writer = vec![];
                ciborium::into_writer(&self, &mut writer)
                    .map_err(|e| MauveError::CborError(e.to_string()))?;
                Ok(writer)
            }

            fn from_object(b: Vec<u8>) -> Result<#name, MauveError> {
                let reader = BufReader::new(&*b);
                let res = ciborium::from_reader(reader)
                    .map_err(|e| MauveError::CborError(e.to_string()))?;
                Ok(res)
            }
        }
    };
    gen.into()
}
