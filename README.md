# Couchbase JDBC Driver

[![license](https://img.shields.io/github/license/couchbase/couchbase-jvm-clients?color=brightgreen)](https://opensource.org/licenses/Apache-2.0)

This project contains the source code for the Couchbase JDBC Driver which supports the Analytics Service (not Query!). Its main purpose is to provide the low-level glue to facilitate integration with high level BI-Tools like Tableau.

## Overview

Right now it is a **work in progress** and can only be obtained through the source code. Also, **no official support** is provided, but feel free to raise a ticket on the issue tracker, and we will try to help.

## Usage: Analytics

 - **Current Version:** `0.3.0`
 - **JDBC URL Prefix:** `jdbc:couchbase:analytics` or `jdbc:cb:analytics` as a fallback.
 - **Minimum Couchbase Server Version:** 7.1.0