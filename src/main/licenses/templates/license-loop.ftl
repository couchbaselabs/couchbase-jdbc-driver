<#--
  Copyright 2025 Couchbase, Inc.

  Shared dependency-iteration loop for the BOM / third-party-list templates.
  Walks the resolved dependency set (licenseMap) and invokes the caller-defined
  <licensecomponent> macro once per artifact. Shadowed (shaded-in) artifacts are
  included only when the caller sets includeShadowed=true.
-->
<#list licenseMap as e>
  <#assign projects = e.getValue().projects/>
  <#list projects as p>
    <#if !p.shadowed || includeShadowed!false>
      <#if p.url??>
        <#assign urlsuffix = " - " + p.url/>
      <#else>
        <#assign urlsuffix = ""/>
      </#if>
      <@licensecomponent name="${p.name}" groupid="${p.groupId}" artifactid="${p.artifactId}" version="${p.version}" urlsuffix="${urlsuffix}" jar="${p.jarName}"/>
    </#if>
  </#list>
</#list>
