Steps to bootstrap a new consumer/provider account with id `ACCOUNT_ID` for the environment `ENV` and hub `HUB`:
1. Set the environment variable `AWS_DEFAULT_REGION=eu-west-1` (This should match the config's default region of the default hub).
2. To enable the S3 backend: Determine the `SECURITY_ACCOUNT_ID` for the given hub from the config. Retrieve the AWS credentials and save it under the profile `prod-security-global-<SECURITY_ACCOUNT_ID>`.
3. With the credentials for the account to be bootstrapped execute:
    ```
     python bootstrap.py --account-id <ACCOUNT_ID> --env <ENV> --hub <HUB> --saml-file-path idp.iam.cdh-oss.bmw.cloud_metadata.xml
    ```

The bootstrapping process can be reverted by executing the command from the last step with the additional parameter `--destroy`.

There also some optional parameters that need to be set in the context of [create-cdh](https://github.com/bmw-cdh/cdh/tree/main/create_cdh):
- `--deployment-prefix`
- `--auth-domain`
- `--security-account-profile`
- `--auto-approve`

When omitted, these default to values that are reasonable in the context of _cdh-oss.bmw.cloud_.
