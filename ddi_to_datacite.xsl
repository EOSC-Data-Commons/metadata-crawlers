<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:oai="http://www.openarchives.org/OAI/2.0/"
                xmlns:ddi="ddi:codebook:2_5"
                xmlns:datacite="http://datacite.org/schema/kernel-4"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                exclude-result-prefixes="ddi">
  
  <xsl:output method="xml" indent="yes" encoding="UTF-8"/>
  
  <!-- Root template -->
  <xsl:template match="/">
    <record xmlns="http://www.openarchives.org/OAI/2.0/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <!-- Copy header -->
      <xsl:element name="header" namespace="http://www.openarchives.org/OAI/2.0/">
        <!-- copy header attributes -->
        <xsl:for-each select="oai:record/oai:header/@*">
          <xsl:attribute name="{name()}">
            <xsl:value-of select="."/>
          </xsl:attribute>
        </xsl:for-each>
        
        <!-- recreate each child element as an OAI element (same local-name), copying attributes and text value only -->
        <xsl:for-each select="oai:record/oai:header/*">
          <xsl:element name="{local-name()}" namespace="http://www.openarchives.org/OAI/2.0/">
            <xsl:copy-of select="@*"/>
            <xsl:value-of select="normalize-space(.)"/>
          </xsl:element>
        </xsl:for-each>
      </xsl:element>

      
      <!-- Transform metadata -->
      <metadata>
        <xsl:apply-templates select="oai:record/oai:metadata/ddi:codeBook | record/metadata/ddi:codeBook"/>
      </metadata>
      
      <!-- Copy about if present -->
      <xsl:copy-of select="oai:record/oai:about | record/about"/>
    </record>
  </xsl:template>
  
  
  <!-- Main DDI to DataCite 4.6 transformation -->
  <xsl:template match="ddi:codeBook">
    <resource xmlns="http://datacite.org/schema/kernel-4"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
              xsi:schemaLocation="http://datacite.org/schema/kernel-4 https://schema.datacite.org/meta/kernel-4.6/metadata.xsd">
      
      <!-- 1. Identifier (Mandatory) - Prefer DOI -->
      <xsl:call-template name="identifier"/>
      
      <!-- 2. Creators (Mandatory) -->
      <xsl:call-template name="creators"/>
      
      <!-- 3. Titles (Mandatory) -->
      <xsl:call-template name="titles"/>
      
      <!-- 4. Publisher (Mandatory) -->
      <xsl:call-template name="publisher"/>
      
      <!-- 5. PublicationYear (Mandatory) -->
      <xsl:call-template name="publicationYear"/>
      
      <!-- 6. ResourceType (Mandatory) -->
      <resourceType resourceTypeGeneral="Dataset">
        <xsl:choose>
          <xsl:when test=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:dataKind">
            <xsl:value-of select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:dataKind"/>
          </xsl:when>
          <xsl:otherwise>Survey Data</xsl:otherwise>
        </xsl:choose>
      </resourceType>
      
      <!-- 7. Subjects (Recommended) -->
      <xsl:call-template name="subjects"/>
      
      <!-- 8. Contributors (Recommended) -->
      <xsl:call-template name="contributors"/>
      
      <!-- 9. Dates (Recommended) -->
      <xsl:call-template name="dates"/>
      
      <!-- 10. Language (Optional) -->
      <xsl:call-template name="language"/>
      
      <!-- 11. AlternateIdentifiers (Optional) -->
      <xsl:call-template name="alternateIdentifiers"/>
      
      <!-- 12. RelatedIdentifiers (Optional) -->
      <xsl:call-template name="relatedIdentifiers"/>
      
      <!-- 13. Sizes (Optional) -->
      <xsl:call-template name="sizes"/>
      
      <!-- 14. Formats (Optional) -->
      <xsl:call-template name="formats"/>
      
      <!-- 15. Version (Optional) -->
      <xsl:call-template name="version"/>
      
      <!-- 16. Rights (Optional) -->
      <xsl:call-template name="rights"/>
      
      <!-- 17. Descriptions (Recommended) -->
      <xsl:call-template name="descriptions"/>
      
      <!-- 18. GeoLocations (Recommended) -->
      <xsl:call-template name="geoLocations"/>
      
      <!-- 19. FundingReferences (Optional) -->
      <xsl:call-template name="fundingReferences"/>
      
    </resource>
  </xsl:template>
  
  <!-- Template: Identifier (Mandatory - Prefer DOI) -->
  <xsl:template name="identifier">
    <xsl:choose>
      <!-- First priority: DOI from IDNo -->
      <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo[@agency='DOI']">
        <datacite:identifier identifierType="DOI">
          <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo[@agency='DOI']"/>
        </datacite:identifier>
      </xsl:when>
      <!-- Second priority: DOI from holdings URI -->
      <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:holdings/@URI[contains(., 'doi.org')]">
        <datacite:identifier identifierType="DOI">
          <xsl:value-of select="substring-after(.//ddi:stdyDscr/ddi:citation/ddi:holdings/@URI, 'doi.org/')"/>
        </datacite:identifier>
      </xsl:when>
      <!-- Third priority: First IDNo available -->
      <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo">
        <datacite:identifier>
          <xsl:attribute name="identifierType">
            <xsl:choose>
              <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo/@agency">
                <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo/@agency"/>
              </xsl:when>
              <xsl:otherwise>Other</xsl:otherwise>
            </xsl:choose>
          </xsl:attribute>
          <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo[1]"/>
        </datacite:identifier>
      </xsl:when>
      <!-- Fallback: Use OAI identifier -->
      <xsl:otherwise>
        <datacite:identifier identifierType="Other">
          <xsl:value-of select="ancestor::oai:record/oai:header/oai:identifier"/>
        </datacite:identifier>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  
  <!-- Template: Creators (Mandatory) -->
  <xsl:template name="creators">
    <datacite:creators>
      <xsl:choose>
        <!-- Authors from rspStmt/AuthEnty -->
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:rspStmt/ddi:AuthEnty">
          <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:rspStmt/ddi:AuthEnty">
            <datacite:creator>
              <datacite:creatorName nameType="Personal">
                <xsl:value-of select="."/>
              </datacite:creatorName>
              <xsl:if test="@affiliation">
                <datacite:affiliation><xsl:value-of select="@affiliation"/></datacite:affiliation>
              </xsl:if>
            </datacite:creator>
          </xsl:for-each>
        </xsl:when>
        <!-- Fallback to producer -->
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:producer">
          <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:producer">
            <datacite:creator>
              <datacite:creatorName nameType="Organizational">
                <xsl:value-of select="."/>
              </datacite:creatorName>
              <xsl:if test="@affiliation">
                <datacite:affiliation><xsl:value-of select="@affiliation"/></datacite:affiliation>
              </xsl:if>
            </datacite:creator>
          </xsl:for-each>
        </xsl:when>
        <!-- Last resort: distributor -->
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distrbtr">
          <datacite:creator>
            <datacite:creatorName nameType="Organizational">
              <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distrbtr[1]"/>
            </datacite:creatorName>
          </datacite:creator>
        </xsl:when>
        <xsl:otherwise>
          <datacite:creator>
            <datacite:creatorName>Unknown</datacite:creatorName>
          </datacite:creator>
        </xsl:otherwise>
      </xsl:choose>
    </datacite:creators>
  </xsl:template>
  
  <!-- Template: Titles (Mandatory) -->
  <xsl:template name="titles">
    <datacite:titles>
      <!-- Main title(s) -->
      <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:titl">
        <datacite:title>
          <xsl:if test="@xml:lang">
            <xsl:attribute name="xml:lang">
              <xsl:value-of select="@xml:lang"/>
            </xsl:attribute>
          </xsl:if>
          <xsl:value-of select="."/>
        </datacite:title>
      </xsl:for-each>
      
      <!-- Parallel titles (translated titles) -->
      <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:parTitl">
        <datacite:title titleType="TranslatedTitle">
          <xsl:if test="@xml:lang">
            <xsl:attribute name="xml:lang">
              <xsl:value-of select="@xml:lang"/>
            </xsl:attribute>
          </xsl:if>
          <xsl:value-of select="."/>
        </datacite:title>
      </xsl:for-each>
      
      <!-- Alternative titles -->
      <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:altTitl">
        <datacite:title titleType="AlternativeTitle">
          <xsl:if test="@xml:lang">
            <xsl:attribute name="xml:lang">
              <xsl:value-of select="@xml:lang"/>
            </xsl:attribute>
          </xsl:if>
          <xsl:value-of select="."/>
        </datacite:title>
      </xsl:for-each>
    </datacite:titles>
  </xsl:template>
  
  <!-- Template: Publisher (Mandatory) -->
  <xsl:template name="publisher">
    <datacite:publisher>
      <xsl:choose>
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distrbtr">
          <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distrbtr[1]"/>
        </xsl:when>
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:producer">
          <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:producer[1]"/>
        </xsl:when>
        <xsl:otherwise>Unknown Publisher</xsl:otherwise>
      </xsl:choose>
    </datacite:publisher>
  </xsl:template>
  
  <!-- Template: PublicationYear (Mandatory) -->
  <xsl:template name="publicationYear">
    <datacite:publicationYear>
      <xsl:choose>
        <!-- Distribution date -->
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distDate/@date">
          <xsl:value-of select="substring(.//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distDate/@date, 1, 4)"/>
        </xsl:when>
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distDate">
          <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distDate"/>
        </xsl:when>
        <!-- Production date -->
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:prodDate/@date">
          <xsl:value-of select="substring(.//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:prodDate/@date, 1, 4)"/>
        </xsl:when>
        <xsl:when test=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:prodDate">
          <xsl:value-of select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:prodDate"/>
        </xsl:when>
        <!-- Collection date -->
        <xsl:when test=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:collDate/@date">
          <xsl:value-of select="substring(.//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:collDate/@date, 1, 4)"/>
        </xsl:when>
        <!-- OAI datestamp -->
        <xsl:when test="ancestor::oai:record/oai:header/oai:datestamp">
          <xsl:value-of select="substring(ancestor::oai:record/oai:header/oai:datestamp, 1, 4)"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="format-dateTime(current-dateTime(), '[Y0001]')"/>
        </xsl:otherwise>
      </xsl:choose>
    </datacite:publicationYear>
  </xsl:template>
  
  <!-- Template: Subjects (Recommended) -->
  <xsl:template name="subjects">
    <xsl:if test=".//ddi:stdyDscr/ddi:stdyInfo/ddi:subject/ddi:keyword or 
      .//ddi:stdyDscr/ddi:stdyInfo/ddi:subject/ddi:topcClas">
      <datacite:subjects>
        <!-- Keywords -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:subject/ddi:keyword">
          <datacite:subject>
            <xsl:if test="@vocab">
              <xsl:attribute name="subjectScheme">
                <xsl:value-of select="@vocab"/>
              </xsl:attribute>
            </xsl:if>
            <xsl:if test="@vocabURI">
              <xsl:attribute name="schemeURI">
                <xsl:value-of select="@vocabURI"/>
              </xsl:attribute>
            </xsl:if>
            <xsl:if test="@xml:lang">
              <xsl:attribute name="xml:lang">
                <xsl:value-of select="@xml:lang"/>
              </xsl:attribute>
            </xsl:if>
            <xsl:value-of select="."/>
          </datacite:subject>
        </xsl:for-each>
        
        <!-- Topic Classifications - Group by language to avoid duplicates -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:subject/ddi:topcClas">
          <datacite:subject>
            <xsl:if test="@vocab">
              <xsl:attribute name="subjectScheme">
                <xsl:value-of select="@vocab"/>
              </xsl:attribute>
            </xsl:if>
            <xsl:if test="@vocabURI">
              <xsl:attribute name="schemeURI">
                <xsl:value-of select="@vocabURI"/>
              </xsl:attribute>
            </xsl:if>
            <xsl:if test="@xml:lang">
              <xsl:attribute name="xml:lang">
                <xsl:value-of select="@xml:lang"/>
              </xsl:attribute>
            </xsl:if>
            <xsl:value-of select="."/>
          </datacite:subject>
        </xsl:for-each>
      </datacite:subjects>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Contributors (Recommended) -->
  <xsl:template name="contributors">
    <xsl:if test=".//ddi:stdyDscr/ddi:citation/ddi:rspStmt/ddi:othId or
      .//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:fundAg or
      .//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:grantNo or
      .//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:contact or
      .//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:depositr or
      .//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:dataCollector or
      .//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:producer">
      <datacite:contributors>
        <!-- Data Collector -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:dataCollector">
          <datacite:contributor contributorType="DataCollector">
            <datacite:contributorName>
              <xsl:if test="contains(., ',')">
                <xsl:attribute name="nameType">Personal</xsl:attribute>
              </xsl:if>
              <xsl:value-of select="."/>
            </datacite:contributorName>
            <xsl:if test="@affiliation">
              <datacite:affiliation><xsl:value-of select="@affiliation"/></datacite:affiliation>
            </xsl:if>
          </datacite:contributor>
        </xsl:for-each>
        
        <!-- Producer (if not already a creator) -->
        <xsl:if test="not(.//ddi:stdyDscr/ddi:citation/ddi:rspStmt/ddi:AuthEnty)">
          <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:producer">
            <datacite:contributor contributorType="Producer">
              <datacite:contributorName nameType="Organizational">
                <xsl:value-of select="."/>
              </datacite:contributorName>
              <xsl:if test="@affiliation">
                <datacite:affiliation><xsl:value-of select="@affiliation"/></datacite:affiliation>
              </xsl:if>
            </datacite:contributor>
          </xsl:for-each>
        </xsl:if>
        
        <!-- Other contributors -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:rspStmt/ddi:othId">
          <datacite:contributor contributorType="Other">
            <datacite:contributorName>
              <xsl:value-of select="."/>
            </datacite:contributorName>
            <xsl:if test="@affiliation">
              <datacite:affiliation><xsl:value-of select="@affiliation"/></datacite:affiliation>
            </xsl:if>
          </datacite:contributor>
        </xsl:for-each>
        
        <!-- Funding Agency -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:fundAg">
          <datacite:contributor contributorType="Funder">
            <datacite:contributorName nameType="Organizational">
              <xsl:value-of select="."/>
            </datacite:contributorName>
            <xsl:if test="@abbr">
              <datacite:affiliation><xsl:value-of select="@abbr"/></datacite:affiliation>
            </xsl:if>
          </datacite:contributor>
        </xsl:for-each>
        
        <!-- Contact Person -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:contact">
          <datacite:contributor contributorType="ContactPerson">
            <datacite:contributorName>
              <xsl:value-of select="."/>
            </datacite:contributorName>
            <xsl:if test="@affiliation">
              <datacite:affiliation><xsl:value-of select="@affiliation"/></datacite:affiliation>
            </xsl:if>
            <xsl:if test="@email">
              <datacite:nameIdentifier nameIdentifierScheme="Email">
                <xsl:value-of select="@email"/>
              </datacite:nameIdentifier>
            </xsl:if>
          </datacite:contributor>
        </xsl:for-each>
        
        <!-- Depositor -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:depositr">
          <datacite:contributor contributorType="DataManager">
            <datacite:contributorName>
              <xsl:value-of select="."/>
            </datacite:contributorName>
            <xsl:if test="@affiliation">
              <datacite:affiliation><xsl:value-of select="@affiliation"/></datacite:affiliation>
            </xsl:if>
          </datacite:contributor>
        </xsl:for-each>
      </datacite:contributors>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Dates (Recommended) -->
  <xsl:template name="dates">
    <xsl:if test=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:prodDate or
      .//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:collDate or
      .//ddi:stdyDscr/ddi:citation/ddi:verStmt/ddi:version/@date or
      .//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:timePrd or
      .//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distDate">
      <datacite:dates>
        <!-- Production Date -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt/ddi:prodDate">
          <datacite:date dateType="Created">
            <xsl:choose>
              <xsl:when test="@date">
                <xsl:value-of select="@date"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="."/>
              </xsl:otherwise>
            </xsl:choose>
          </datacite:date>
        </xsl:for-each>
        
        <!-- Distribution Date -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:distStmt/ddi:distDate">
          <datacite:date dateType="Issued">
            <xsl:choose>
              <xsl:when test="@date">
                <xsl:value-of select="@date"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="."/>
              </xsl:otherwise>
            </xsl:choose>
          </datacite:date>
        </xsl:for-each>
        
        <!-- Collection Date -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:collDate">
          <datacite:date dateType="Collected">
            <xsl:choose>
              <xsl:when test="@date">
                <xsl:value-of select="@date"/>
              </xsl:when>
              <xsl:when test="@event='start' and following-sibling::ddi:collDate[@event='end']">
                <xsl:value-of select="@date"/>/<xsl:value-of select="following-sibling::ddi:collDate[@event='end'][1]/@date"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="."/>
              </xsl:otherwise>
            </xsl:choose>
          </datacite:date>
        </xsl:for-each>
        
        <!-- Version Date -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:verStmt/ddi:version[@date]">
          <datacite:date dateType="Updated">
            <xsl:value-of select="@date"/>
          </datacite:date>
        </xsl:for-each>
        
        <!-- Time Period Covered -->
        <xsl:if test=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:timePrd[@event='start'] and 
          .//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:timePrd[@event='end']">
          <datacite:date dateType="Other" dateInformation="Time Period Covered">
            <xsl:value-of select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:timePrd[@event='start']/@date"/>/<xsl:value-of select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:timePrd[@event='end']/@date"/>
          </datacite:date>
        </xsl:if>
        
        <!-- Single time period -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:stdyInfo/ddi:sumDscr/ddi:timePrd[not(@event)]">
          <datacite:date dateType="Other" dateInformation="Time Period Covered">
            <xsl:choose>
              <xsl:when test="@date">
                <xsl:value-of select="@date"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:value-of select="."/>
              </xsl:otherwise>
            </xsl:choose>
          </datacite:date>
        </xsl:for-each>
      </datacite:dates>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Language (Optional) -->
  <xsl:template name="language">
    <xsl:if test=".//ddi:codeBook/@xml:lang">
      <datacite:language>
        <xsl:value-of select=".//ddi:codeBook/@xml:lang"/>
      </datacite:language>
    </xsl:if>
  </xsl:template>
  
  
  <!-- ============================
       New templates filled in below
       ============================ -->
  
  <!-- Template: AlternateIdentifiers -->
  <xsl:template name="alternateIdentifiers">
    <xsl:variable name="idnos" select=".//ddi:stdyDscr/ddi:citation/ddi:titlStmt/ddi:IDNo[not(@agency='DOI')]"/>
    <xsl:if test="$idnos or .//ddi:stdyDscr/ddi:citation/ddi:holdings[not(contains(@URI,'doi.org'))]/@URI">
      <datacite:alternateIdentifiers>
        <!-- IDNo elements other than DOI -->
        <xsl:for-each select="$idnos">
          <datacite:alternateIdentifier>
            <xsl:attribute name="alternateIdentifierType">
              <xsl:choose>
                <xsl:when test="@agency"><xsl:value-of select="@agency"/></xsl:when>
                <xsl:otherwise>Other</xsl:otherwise>
              </xsl:choose>
            </xsl:attribute>
            <xsl:value-of select="."/>
          </datacite:alternateIdentifier>
        </xsl:for-each>
        <!-- holdings URIs that are not DOI -->
        <xsl:for-each select=".//ddi:stdyDscr/ddi:citation/ddi:holdings[not(contains(@URI,'doi.org'))]">
          <xsl:if test="@URI">
            <datacite:alternateIdentifier alternateIdentifierType="URL">
              <xsl:value-of select="@URI"/>
            </datacite:alternateIdentifier>
          </xsl:if>
        </xsl:for-each>
      </datacite:alternateIdentifiers>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: RelatedIdentifiers -->
  <xsl:template name="relatedIdentifiers">
    <!-- Map holdings URIs and any reference-like elements to relatedIdentifiers -->
    <xsl:variable name="holdings" select=".//ddi:stdyDscr/ddi:citation/ddi:holdings[@URI]"/>
    <xsl:variable name="refs" select=".//ddi:stdyDscr//ddi:bibCit | .//ddi:stdyDscr//ddi:othId"/>
    <xsl:if test="$holdings or $refs">
      <datacite:relatedIdentifiers>
        <!-- holdings: if holds DOI then it's already identifier; otherwise a relatedIdentifier -->
        <xsl:for-each select="$holdings">
          <xsl:variable name="u" select="@URI"/>
          <xsl:choose>
            <xsl:when test="contains($u,'doi.org')">
              <!-- DOI holdings - some repos prefer these in identifier; skip here to avoid duplicate -->
            </xsl:when>
            <xsl:otherwise>
              <datacite:relatedIdentifier relatedIdentifierType="URL" relationType="References">
                <xsl:value-of select="@URI"/>
              </datacite:relatedIdentifier>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:for-each>
        <!-- bibliographic citations / other ids -->
        <xsl:for-each select="$refs">
          <xsl:variable name="text" select="normalize-space(.)"/>
          <xsl:if test="$text">
            <datacite:relatedIdentifier relatedIdentifierType="Text" relationType="References">
              <xsl:value-of select="$text"/>
            </datacite:relatedIdentifier>
          </xsl:if>
        </xsl:for-each>
      </datacite:relatedIdentifiers>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Sizes -->
  <xsl:template name="sizes">
    <!-- Try common DDI file size locations -->
    <xsl:variable name="sizes" select=".//ddi:fileDscr//ddi:fileTxt//ddi:fileSize | .//ddi:fileDscr//ddi:fileTxt//ddi:size | .//ddi:fileDscr//ddi:fileTxt//ddi:bytes"/>
    <xsl:if test="$sizes">
      <datacite:sizes>
        <xsl:for-each select="$sizes">
          <datacite:size>
            <xsl:value-of select="normalize-space(.)"/>
          </datacite:size>
        </xsl:for-each>
      </datacite:sizes>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Formats -->
  <xsl:template name="formats">
    <xsl:variable name="formats" select=".//ddi:fileDscr//ddi:fileTxt//ddi:fileFormat | .//ddi:fileDscr//ddi:fileTxt//ddi:fileFormat/@format"/>
    <xsl:variable name="filenames" select=".//ddi:fileDscr//ddi:fileTxt//ddi:fileName"/>
    <xsl:if test="$formats or $filenames">
      <datacite:formats>
        <!-- explicit formats -->
        <xsl:for-each select="$formats">
          <datacite:format>
            <xsl:value-of select="normalize-space(.)"/>
          </datacite:format>
        </xsl:for-each>
        <!-- fallback: derive from fileName extension if present -->
        <xsl:for-each select="$filenames">
          <xsl:variable name="fn" select="normalize-space(.)"/>
          <xsl:if test="$fn and contains($fn,'.')">
            <datacite:format>
              <xsl:value-of select="substring-after($fn, concat('.', substring-after($fn, '.')))"/>
            </datacite:format>
          </xsl:if>
        </xsl:for-each>
      </datacite:formats>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Version -->
  <xsl:template name="version">
    <xsl:variable name="ver" select=".//ddi:stdyDscr/ddi:citation/ddi:verStmt/ddi:version"/>
    <xsl:if test="$ver">
      <xsl:for-each select="$ver">
        <datacite:version>
          <xsl:choose>
            <xsl:when test="@version"><xsl:value-of select="@version"/></xsl:when>
            <xsl:when test="string-length(normalize-space(.)) &gt; 0"><xsl:value-of select="normalize-space(.)"/></xsl:when>
            <xsl:otherwise>1.0</xsl:otherwise>
          </xsl:choose>
        </datacite:version>
      </xsl:for-each>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Rights -->
  <xsl:template name="rights">
    <!-- Map useStmt/restrctn elements to rightsList/rights -->
    <xsl:variable name="restrictions" select=".//ddi:stdyDscr//ddi:dataAccs//ddi:useStmt//ddi:restrctn"/>
    <xsl:if test="$restrictions">
      <datacite:rightsList>
        <xsl:for-each select="$restrictions">
          <datacite:rights>
            <!-- If element has xml:lang preserve it -->
            <xsl:if test="@xml:lang">
              <xsl:attribute name="xml:lang"><xsl:value-of select="@xml:lang"/></xsl:attribute>
            </xsl:if>
            <xsl:value-of select="normalize-space(.)"/>
          </datacite:rights>
        </xsl:for-each>
      </datacite:rightsList>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: Descriptions -->
  <xsl:template name="descriptions">
    <xsl:variable name="abstracts" select=".//ddi:stdyDscr//ddi:stdyInfo//ddi:abstract | .//ddi:docDscr//ddi:citation//ddi:abstract"/>
    <xsl:variable name="sumDscrs" select=".//ddi:stdyDscr//ddi:stdyInfo//ddi:sumDscr"/>
    <xsl:if test="$abstracts or $sumDscrs">
      <datacite:descriptions>
        <!-- Abstracts -->
        <xsl:for-each select="$abstracts">
          <datacite:description descriptionType="Abstract">
            <xsl:if test="@xml:lang"><xsl:attribute name="xml:lang"><xsl:value-of select="@xml:lang"/></xsl:attribute></xsl:if>
            <xsl:value-of select="normalize-space(.)"/>
          </datacite:description>
        </xsl:for-each>
        <!-- Summary descriptions: put as Other -->
        <xsl:for-each select="$sumDscrs">
          <!-- Use sumDscr elements Other -->
          <xsl:for-each select="ddi:notes">
            <datacite:description descriptionType="Other">
              <xsl:if test="@xml:lang"><xsl:attribute name="xml:lang"><xsl:value-of select="@xml:lang"/></xsl:attribute></xsl:if>
              <xsl:value-of select="normalize-space(.)"/>
            </datacite:description>
          </xsl:for-each>
        </xsl:for-each>
      </datacite:descriptions>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: GeoLocations -->
  <xsl:template name="geoLocations">
    <!-- Map sumDscr/nation elements to geoLocationPlace -->
    <xsl:variable name="nations" select=".//ddi:stdyDscr//ddi:stdyInfo//ddi:sumDscr//ddi:nation"/>
    <xsl:if test="$nations">
      <datacite:geoLocations>
        <xsl:for-each select="$nations">
          <datacite:geoLocation>
            <datacite:geoLocationPlace>
              <xsl:if test="@xml:lang"><xsl:attribute name="xml:lang"><xsl:value-of select="@xml:lang"/></xsl:attribute></xsl:if>
              <xsl:value-of select="normalize-space(.)"/>
            </datacite:geoLocationPlace>
          </datacite:geoLocation>
        </xsl:for-each>
      </datacite:geoLocations>
    </xsl:if>
  </xsl:template>
  
  <!-- Template: FundingReferences -->
  <xsl:template name="fundingReferences">
    <!-- Map prodStmt/fundAg and prodStmt/grantNo -->
    <xsl:variable name="funders" select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt//ddi:fundAg"/>
    <xsl:variable name="grants" select=".//ddi:stdyDscr/ddi:citation/ddi:prodStmt//ddi:grantNo"/>
    <xsl:if test="$funders or $grants">
      <datacite:fundingReferences>
        <!-- iterate fundAg with potential matching grantNo by position -->
        <xsl:for-each select="$funders">
          <datacite:fundingReference>
            <datacite:funderName>
              <xsl:value-of select="normalize-space(.)"/>
            </datacite:funderName>
            <xsl:if test="@abbr">
              <datacite:funderIdentifier>
                <xsl:value-of select="@abbr"/>
              </datacite:funderIdentifier>
            </xsl:if>
            <!-- try to pick up a grantNo sibling or following grantNo -->
            <xsl:variable name="g" select="following-sibling::ddi:grantNo[1] | preceding-sibling::ddi:grantNo[1]"/>
            <xsl:if test="$g">
              <xsl:for-each select="$g">
                <xsl:if test="normalize-space(.)">
                  <datacite:awardNumber>
                    <xsl:value-of select="normalize-space(.)"/>
                  </datacite:awardNumber>
                </xsl:if>
              </xsl:for-each>
            </xsl:if>
          </datacite:fundingReference>
        </xsl:for-each>
        <!-- any grantNos not associated with a fundAg -->
        <xsl:for-each select="$grants[not(preceding-sibling::ddi:fundAg or following-sibling::ddi:fundAg)]">
          <datacite:fundingReference>
            <datacite:awardNumber>
              <xsl:value-of select="normalize-space(.)"/>
            </datacite:awardNumber>
          </datacite:fundingReference>
        </xsl:for-each>
      </datacite:fundingReferences>
    </xsl:if>
  </xsl:template>
  
</xsl:stylesheet>
