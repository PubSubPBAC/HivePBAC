package de.karlw.pbac.auth;

import com.hivemq.extension.sdk.api.auth.SimpleAuthenticator;
import com.hivemq.extension.sdk.api.auth.parameter.SimpleAuthInput;
import com.hivemq.extension.sdk.api.auth.parameter.SimpleAuthOutput;

public class PBACAuth implements SimpleAuthenticator {

    @Override
    public void onConnect(SimpleAuthInput simpleAuthInput, SimpleAuthOutput simpleAuthOutput) {
            simpleAuthOutput.authenticateSuccessfully();
//            simpleAuthOutput.failAuthentication();
    }
}