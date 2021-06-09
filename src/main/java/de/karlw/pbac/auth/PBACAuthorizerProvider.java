package de.karlw.pbac.auth;

import com.hivemq.extension.sdk.api.auth.Authorizer;
import com.hivemq.extension.sdk.api.auth.parameter.AuthorizerProviderInput;
import com.hivemq.extension.sdk.api.services.auth.provider.AuthorizerProvider;

public class PBACAuthorizerProvider  implements AuthorizerProvider {

    //create a shared instance of SubscriptionAuthorizer
    private final APSubscriptionAuthorizer apSubscriptionAuthorizer;

    public PBACAuthorizerProvider(APSubscriptionAuthorizer apSubscriptionAuthorizer) {
        this.apSubscriptionAuthorizer = apSubscriptionAuthorizer;
    }

    @Override
    public Authorizer getAuthorizer(AuthorizerProviderInput authorizerProviderInput) {
        //always return the shared instance.
        //It is also possible to create a new instance here for each client
        return apSubscriptionAuthorizer;
    }
}