package de.karlw.pbac.auth;

import com.hivemq.extension.sdk.api.auth.Authenticator;
import com.hivemq.extension.sdk.api.auth.parameter.AuthenticatorProviderInput;
import com.hivemq.extension.sdk.api.services.auth.provider.AuthenticatorProvider;

public class PBACAuthProvider implements AuthenticatorProvider {

    @Override
    public Authenticator getAuthenticator(AuthenticatorProviderInput authenticatorProviderInput) {
        //return an instance of an Authenticator
        return new PBACAuth();
    }

}