<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">
##<center><H1><font color="darkorchid4">Mobius API Documentation<!--xsl:value-of select="$AssemblyName"/--></font></H1></center>
<xsl:apply-templates select="//member[contains(@name,'T:') and not(contains(@name,'Helper')) and not(contains(@name,'Wrapper')) and not(contains(@name,'Configuration')) and not(contains(@name,'Proxy')) and not(contains(@name,'Interop')) and not(contains(@name,'Services'))]"/>
</xsl:template>

<xsl:template match="//member[contains(@name,'T:') and not(contains(@name,'Helper')) and not(contains(@name,'Wrapper')) and not(contains(@name,'Configuration')) and not(contains(@name,'Proxy')) and not(contains(@name,'Interop')) and not(contains(@name,'Services'))]">
  <xsl:variable name="TypeName" select="substring-after(@name, ':')"/>
###<font color="#68228B"><xsl:value-of select="$TypeName"/></font>
####Summary
  <xsl:apply-templates/>

	<xsl:if test="//member[contains(@name,concat('M:',$TypeName))]">
####Methods

<table><tr><th>Name</th><th>Description</th></tr>
		<xsl:for-each select="//member[contains(@name,concat('M:',$TypeName)) and not(contains(@name,'#ctor'))]">
		<xsl:variable name="MethodName" select="substring-after(@name, concat('M:',$TypeName,'.'))"/>
		<xsl:variable name="MethodNameFormat1" select="substring-before($MethodName, '(')"/>
		<xsl:variable name="MethodNameFormat2" select="substring-before($MethodName, '`')"/>
		<xsl:variable name="MethodSummary" select="summary"/>
<tr>
		<xsl:if test="string-length($MethodNameFormat1) &gt; 0">
<td><font color="blue"><xsl:value-of select="$MethodNameFormat1"/></font></td>
		</xsl:if>
		<xsl:if test="(string-length($MethodNameFormat1) = 0) and (string-length($MethodNameFormat2) &gt; 0)">
<td><font color="blue"><xsl:value-of select="$MethodNameFormat2"/></font></td>
		</xsl:if>
		<xsl:if test="(string-length($MethodNameFormat1) = 0) and (string-length($MethodNameFormat2) = 0)">
<td><font color="blue"><xsl:value-of select="$MethodName"/></font></td>
		</xsl:if>			
<td><xsl:value-of select="normalize-space($MethodSummary)"/></td>
</tr>
		
      </xsl:for-each>
</table>

---
  
  </xsl:if>
 
</xsl:template>
</xsl:stylesheet>
