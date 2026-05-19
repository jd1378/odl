use secrecy::{ExposeSecret, SecretString};
use std::fmt;

#[derive(Clone)]
pub struct Credentials {
    username: String,
    password: Option<SecretString>,
}

impl PartialEq for Credentials {
    fn eq(&self, other: &Self) -> bool {
        self.username == other.username
        // no need for constant time comparisons, not sensitive context
            && self.password.as_ref().map(|x| x.expose_secret()) == other.password.as_ref().map(|x| x.expose_secret())
    }
}

impl Eq for Credentials {}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"***")
            .finish()
    }
}

impl Credentials {
    pub fn new(username: &str, password: Option<&str>) -> Credentials {
        Credentials {
            username: username.to_string(),
            password: password.map(SecretString::from),
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> Option<&str> {
        if let Some(password) = &self.password {
            Some(password.expose_secret())
        } else {
            None
        }
    }
}
