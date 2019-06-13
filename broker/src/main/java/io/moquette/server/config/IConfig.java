/*
 * Copyright (c) 2012-2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.server.config;

import io.moquette.BrokerConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Base interface for all configuration implementations (filesystem, memory or classpath)
 */
public abstract class IConfig {

    public static final String DEFAULT_CONFIG = "config/moquette.conf";
    public static final String LIST_DELIMITER = ",";
    Properties m_properties = new Properties();

    /**
     * Same interface as {@link Properties}
     *
     * @param name  property name
     * @param value property value
     */
    public void setProperty(String name, String value) {
        m_properties.setProperty(name, value);
    }

    /**
     * Same interface as {@link Properties}
     *
     * @param name property name.
     * @return property value.
     */
    public String getProperty(String name) {
        return m_properties.getProperty(name);
    }

    /**
     * Same interface as {@link Properties}
     *
     * @param name         property name.
     * @param defaultValue default value to return in case the property doesn't exists.
     * @return property value.
     */
    public String getProperty(String name, String defaultValue) {
        return m_properties.getProperty(name, defaultValue);
    }

    /**
     * Reads property value from config and splits it on ",". All whitespace is removed
     *
     * @param name Property name
     * @return Value list from property. Empty list if property is not present
     */
    public List<String> getPropertyAsList(String name) {
        String value = m_properties.getProperty(name);
        if (value == null || value.equals("")) {
            return new ArrayList<>();
        }
        value = value.replaceAll("\\s+", "");
        return Arrays.asList(value.split(LIST_DELIMITER));
    }

    void assignDefaults() {
        setProperty(BrokerConstants.PORT_PROPERTY_NAME, Integer.toString(BrokerConstants.PORT));
        setProperty(BrokerConstants.HOST_PROPERTY_NAME, BrokerConstants.HOST);
        // setProperty(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME,
        // Integer.toString(BrokerConstants.WEBSOCKET_PORT));
        setProperty(BrokerConstants.PASSWORD_FILE_PROPERTY_NAME, "");
        // setProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME,
        // BrokerConstants.DEFAULT_PERSISTENT_PATH);
        setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, Boolean.TRUE.toString());
        setProperty(BrokerConstants.AUTHENTICATOR_CLASS_NAME, "");
        setProperty(BrokerConstants.AUTHORIZATOR_CLASS_NAME, "");
        setProperty(BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
            String.valueOf(BrokerConstants.DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE));
    }

    public abstract IResourceLoader getResourceLoader();

    public int intProp(String propertyName, int defaultValue) {
        String propertyValue = getProperty(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }
        return Integer.parseInt(propertyValue);
    }

    public boolean boolProp(String propertyName, boolean defaultValue) {
        String propertyValue = getProperty(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(propertyValue);
    }
}
