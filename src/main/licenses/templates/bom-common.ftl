<#--
  Copyright 2025 Couchbase, Inc.

  BOM body: emits "groupId:artifactId:version" once per dependency. Callers
  (bom.ftl / bom-no-shadowed.ftl) set includeShadowed before including this.
-->
<#macro licensecomponent name jar description="" groupid="" artifactid="" version="" urlsuffix="">
<#if artifactid != "">
${groupid}:${artifactid}:${version}
</#if>
</#macro>
<#macro licensecontent></#macro>
<#include "license-loop.ftl">
