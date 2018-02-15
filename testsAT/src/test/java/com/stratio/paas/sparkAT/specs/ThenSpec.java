
package com.stratio.paas.sparkAT.specs;

import com.stratio.qa.specs.CommonG;
import com.stratio.qa.utils.ThreadProperty;
import cucumber.api.java.en.Then;
import org.json.JSONObject;

public class ThenSpec {

    protected CommonG commonspec;

    public CommonG getCommonSpec() {
        return this.commonspec;
    }

    public ThenSpec(CommonG spec) {
        this.commonspec = spec;
    }

    @Then(value = "^I save the value from field in service response '(.*?)' in variable '(.*?)'$")
    public void assertResponseMessage(String jsonField, String envVar) {

        String response = commonspec.getResponse().getResponse();
        commonspec.getLogger().debug("Trying to parse JSON from response into object " + response);

        JSONObject json = new JSONObject(response);

        commonspec.getLogger().debug("JSON object parsed successfuly. Trying to recover key "+jsonField);

        String jsonValue = json.getString(jsonField);
        commonspec.getLogger().debug("Json value retrieved from JSON: "+jsonValue);

        commonspec.getLogger().debug("Saving in envVar "+envVar);

        ThreadProperty.set(envVar, jsonValue);
    }
}