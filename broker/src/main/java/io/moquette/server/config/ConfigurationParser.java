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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.Properties;

/**
 * Mosquitto configuration parser.
 * <p>
 * A line that at the very first has # is a comment Each line has key value format, where the
 * separator used it the space.
 */
class ConfigurationParser {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationParser.class);

    private Properties m_properties = new Properties();

    /**
     * Parse the configuration from file.
     */
    void parse(File file) throws ParseException {
        if (file == null) {
            LOG.warn("parsing NULL file, so fallback on default configuration!");
            return;
        }
        if (!file.exists()) {
            LOG.warn(
                String.format(
                    "parsing not existing file %s, so fallback on default configuration!",
                    file.getAbsolutePath()));
            return;
        }
        try {
            Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
            parse(reader);
        } catch (IOException fex) {
            LOG.warn("parsing not existing file {}, fallback on default configuration!", file.getAbsolutePath(), fex);
        }
    }

    /**
     * Parse the configuration
     *
     * @throws IOException if the format is not compliant.
     */
    void parse(Reader reader) throws IOException {
        if (reader == null) {
            // just log and return default properties
            LOG.warn("parsing NULL reader, so fallback on default configuration!");
            return;
        }

        m_properties.load(reader);
    }

    Properties getProperties() {
        return m_properties;
    }
}
