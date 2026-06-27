<#--
  Copyright 2025 Couchbase, Inc.

  Third-party jar list (one jar name per line), including shadowed artifacts.
  Intended as the authoritative input for a license-coverage IT: assert that
  every jar here is accounted for, to catch shaded-in artifacts we'd otherwise
  miss.
-->
<#macro licensecomponent name jar groupid="" artifactid="" version="" urlsuffix="" description="">
<#if jar?has_content && jar != "N/A">
${jar}
</#if>
</#macro>
<#macro licensecontent></#macro>
<#assign includeShadowed = true/>
<#include "license-loop.ftl">
