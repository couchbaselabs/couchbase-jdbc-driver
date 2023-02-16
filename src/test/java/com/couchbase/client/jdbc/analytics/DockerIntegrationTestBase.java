/*
 * Copyright 2023 Couchbase, Inc.
 */
package com.couchbase.client.jdbc.analytics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.jdbc.CouchbaseDriver;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Starts Couchbase Docker Container for Integration Tests
 */
public abstract class DockerIntegrationTestBase {

    private static final Logger LOGGER = CouchbaseDriver.PARENT_LOGGER;

    private static final String SERVER_IMAGE_REPO = "build-docker.couchbase.com";
    private static final String SERVER_IMAGE_PATH = "couchbase/server-internal";
    protected static final String SERVER_USER = "couchbase";
    protected static final String SERVER_PASSWORD = "couchbase";
    private static final String COUCHBASE_SERVER_VERSION_PROPERTY = "server.version";
    private static final String COUCHBASE_SERVER_VERSION_DEFAULT = "7.1.3";
    private static final String COUCHBASE_SERVER_BUILD_PROPERTY = "server.build";
    private static final String COUCHBASE_SERVER_BUILD_DEFAULT = "3481";
    private static final String CBAS_QUOTA_PROPERTY = "cbas.quota";
    private static final int CBAS_QUOTA_DEFAULT = 1200; // MB
    protected static final Duration WAIT_TIMEOUT = Duration.ofMinutes(2);
    private static final String COUCHBASE_LOGS_DIR = "/opt/couchbase/var/lib/couchbase/logs/";
    private static final String TARGET_DIR_PATH = "target";
    private static String testTargetDirPath;

    protected static CouchbaseContainer container;

    public static void startContainer() {
        String serverVersion = System.getProperty(COUCHBASE_SERVER_VERSION_PROPERTY, COUCHBASE_SERVER_VERSION_DEFAULT);
        String serverBuild = System.getProperty(COUCHBASE_SERVER_BUILD_PROPERTY, COUCHBASE_SERVER_BUILD_DEFAULT);
        String serverImageVersion = serverVersion + "-" + serverBuild;
        testTargetDirPath = TARGET_DIR_PATH;

        int cbasMemoryQuota = getCbasMemoryQuota();

        LOGGER.log(Level.INFO, "Starting Couchbase Server {0} inside test container (CBAS memory quota: {1} MB)",
                new Object[] {serverImageVersion, cbasMemoryQuota});

        DockerImageName serverImageName = DockerImageName
                .parse(String.format("%s/%s:%s", SERVER_IMAGE_REPO, SERVER_IMAGE_PATH, serverImageVersion))
                .asCompatibleSubstituteFor("couchbase/server");
        container = new CouchbaseContainer(serverImageName)
                .withEnabledServices(CouchbaseService.KV, CouchbaseService.ANALYTICS)
                .withServiceQuota(CouchbaseService.ANALYTICS, cbasMemoryQuota)
                .withCredentials(SERVER_USER, SERVER_PASSWORD);
        container.start();

        Cluster cluster =  Cluster.connect(container.getConnectionString(), SERVER_USER, SERVER_PASSWORD);
        cluster.waitUntilReady(WAIT_TIMEOUT);
        cluster.analyticsQuery("Select 1");
        cluster.disconnect();

        LOGGER.info("Server connection string:" + container.getConnectionString());
        LOGGER.info("Server started");
    }

    public static void startContainer(Class<? extends DockerIntegrationTestBase> testClass) {
        startContainer();
        testTargetDirPath = String.valueOf(Paths.get(TARGET_DIR_PATH, testClass.getName()));
        File testTargetDir = new File(testTargetDirPath);
        FileUtils.deleteQuietly(testTargetDir);
        testTargetDir.mkdirs();
    }

    private static int getCbasMemoryQuota() {
        String v = System.getProperty(CBAS_QUOTA_PROPERTY, String.valueOf(CBAS_QUOTA_DEFAULT));
        return Integer.parseInt(v);
    }

    public static void stopContainer() {
        if (container != null) {
            copyCouchbaseLogsFromContainer();
            container.stop();
        }
    }

    private static void copyCouchbaseLogsFromContainer() {
        try {
            String containerTarFileName = container.getContainerId() + ".tar";
            String containerTarFilePath = "/tmp/" + containerTarFileName;
            String localTarFilePath =  String.valueOf(Paths.get(testTargetDirPath, containerTarFileName));

            LOGGER.info(String.format("Copying files from docker container %s: %s to local %s", container.getContainerId(),
                    COUCHBASE_LOGS_DIR, testTargetDirPath));

            container.execInContainer("tar", "-cvf", containerTarFilePath, COUCHBASE_LOGS_DIR);
            container.copyFileFromContainer(containerTarFilePath, localTarFilePath);

            File localTarFile = new File(localTarFilePath);
            File outputDir = new File(testTargetDirPath);
            untar(localTarFile, outputDir);
        }
        catch (IOException | InterruptedException e) {
            LOGGER.log(Level.WARNING, String.format("Error copying files from docker container: %s", container.getContainerId()), e);
        }
    }

    private static void untar(File tarFile, File outputDir) throws IOException {
        try (TarArchiveInputStream tis = new TarArchiveInputStream(new FileInputStream(tarFile))) {
            TarArchiveEntry entry;
            while ((entry = tis.getNextTarEntry()) != null) {
                if (!entry.isDirectory()) {
                    String name = entry.getName();
                    File outputFile = new File(outputDir, name);
                    outputFile.getParentFile().mkdirs();
                    try (FileOutputStream os = new FileOutputStream(outputFile)) {
                        IOUtils.copy(tis, os);
                    }
                }
            }
        }
    }
}
