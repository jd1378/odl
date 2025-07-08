use keyring;
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
            password: password.map(|x| SecretString::from(x)),
        }
    }

    pub fn username(&self) -> &str {
        return &self.username;
    }

    pub fn password(&self) -> Option<&str> {
        if let Some(password) = &self.password {
            return Some(password.expose_secret());
        }
        return None;
    }

    /// Does nothing if credentials contain no password
    pub async fn save_to_keyring(&self, target: &str) -> Result<(), keyring::Error> {
        if let Some(password) = &self.password {
            let username = self.username.clone();
            let password = password.expose_secret().to_owned();
            let target = target.to_owned();

            // Spawn blocking task to avoid blocking async runtime
            return tokio::task::spawn_blocking(move || {
                let entry = keyring::Entry::new_with_target(&target, "odl-rs", &username)?;
                entry.set_password(&password)
            })
            .await
            .map_err(|e| {
                keyring::Error::PlatformFailure(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))
            })?;
        }
        return Ok(());
    }

    /// If no entry exists, password will be None
    pub async fn load_from_keyring(username: &str, target: &str) -> Result<Self, keyring::Error> {
        let username = username.to_owned();
        let target = target.to_owned();
        let username_for_keyring = username.clone();

        let password = match tokio::task::spawn_blocking(move || {
            let entry = keyring::Entry::new_with_target(&target, "odl-rs", &username_for_keyring)?;
            entry.get_password()
        })
        .await
        .map_err(|e| {
            keyring::Error::PlatformFailure(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            )))
        })? {
            Ok(pw) => Some(SecretString::from(pw)),
            Err(keyring::Error::NoEntry) => None,
            Err(e) => return Err(e),
        };

        Ok(Credentials { username, password })
    }
}
