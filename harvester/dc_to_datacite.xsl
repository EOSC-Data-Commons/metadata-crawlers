<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:oai="http://www.openarchives.org/OAI/2.0/"
    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
    xmlns:dc="http://purl.org/dc/elements/1.1/"
    xmlns:datacite="http://datacite.org/schema/kernel-4"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    version="2.0"
    exclude-result-prefixes="oai oai_dc dc">

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

            <!-- ================= METADATA WRAPPER ================= -->
            <metadata>
                <xsl:apply-templates select="oai:record/oai:metadata/oai_dc:dc | oai:record/oai:metadata/dc:dc"/>
            </metadata>

            <!-- ================= ABOUT (OPTIONAL) ================= -->
            <xsl:copy-of select="//oai:about"/>

        </record>
    </xsl:template>


    <xsl:template match="oai_dc:dc | dc:dc">
        <resource xmlns="http://datacite.org/schema/kernel-4"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
              xsi:schemaLocation="http://datacite.org/schema/kernel-4 https://schema.datacite.org/meta/kernel-4.6/metadata.xsd">
      

            <xsl:call-template name="identifier"/>
            <xsl:call-template name="creators"/>
            <xsl:call-template name="titles"/>
            <xsl:call-template name="publisher"/>
            <xsl:call-template name="publicationYear"/>
            <xsl:call-template name="resourceType"/>

            <xsl:call-template name="subjects"/>
            <xsl:call-template name="contributors"/>
            <xsl:call-template name="dates"/>
            <xsl:call-template name="language"/>
            <xsl:call-template name="alternateIdentifiers"/>
            <xsl:call-template name="relatedIdentifiers"/>
            <xsl:call-template name="rights"/>
            <xsl:call-template name="descriptions"/>

        </resource>
    </xsl:template>


    <!--  ================= IDENTIFIER (Mandatory) =================  -->
    <xsl:template name="identifier">
        <xsl:choose>
            <!-- DOI takes priority -->
            <xsl:when test="dc:identifier[contains(., 'doi.org') or contains(., '10.')]">
                <datacite:identifier identifierType="DOI">
                    <xsl:value-of select="dc:identifier[contains(., 'doi.org') or contains(., '10.')][1]"/>
                </datacite:identifier>
            </xsl:when>
            <!-- Everything else: use first identifier as URL -->
            <xsl:when test="dc:identifier">
                <datacite:identifier identifierType="URL">
                    <xsl:value-of select="dc:identifier[1]"/>
                </datacite:identifier>
            </xsl:when>
            <xsl:otherwise>
                <datacite:identifier identifierType="Other">Unknown</datacite:identifier>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <!--  ================= CREATORS (Mandatory) =================  -->
    <xsl:template name="creators">
        <datacite:creators>
            <xsl:choose>
                <xsl:when test="dc:creator">
                    <xsl:for-each select="dc:creator">
                        <datacite:creator>
                            <datacite:creatorName>
                                <xsl:attribute name="nameType">
                                    <!--  Heuristic: "Last, First" pattern = Personal  -->
                                    <xsl:choose>
                                        <xsl:when test="contains(., ',')">Personal</xsl:when>
                                        <xsl:otherwise>Organizational</xsl:otherwise>
                                    </xsl:choose>
                                </xsl:attribute>
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:creatorName>
                        </datacite:creator>
                    </xsl:for-each>
                </xsl:when>
                <xsl:otherwise>
                    <datacite:creator>
                        <datacite:creatorName>Unknown</datacite:creatorName>
                    </datacite:creator>
                </xsl:otherwise>
            </xsl:choose>
        </datacite:creators>
    </xsl:template>
    <!--  ================= TITLES (Mandatory) =================  -->
    <xsl:template name="titles">
        <datacite:titles>
            <xsl:for-each select="dc:title">
                <datacite:title>
                    <xsl:if test="@xml:lang">
                        <xsl:attribute name="xml:lang">
                            <xsl:value-of select="@xml:lang"/>
                        </xsl:attribute>
                    </xsl:if>
                    <xsl:value-of select="normalize-space(.)"/>
                </datacite:title>
            </xsl:for-each>
        </datacite:titles>
    </xsl:template>
    <!--  ================= PUBLISHER (Mandatory) =================  -->
    <xsl:template name="publisher">
        <datacite:publisher>
            <xsl:choose>
                <xsl:when test="dc:publisher">
                    <xsl:value-of select="normalize-space(dc:publisher[1])"/>
                </xsl:when>
                <xsl:otherwise>Unknown Publisher</xsl:otherwise>
            </xsl:choose>
        </datacite:publisher>
    </xsl:template>
    <!--  ================= PUBLICATION YEAR (Mandatory) =================  -->
    <!-- 
        DataCite requires a 4-digit year only.
        dc:date may come in as YYYY, YYYY-MM-DD, or YYYY-MM-DDTHH:MM:SSZ — always extract first 4 chars.
     -->
    <xsl:template name="publicationYear">
        <datacite:publicationYear>
            <xsl:variable name="raw" select="normalize-space(dc:date[1])"/>

            <!-- strip label prefix if present, otherwise use as-is -->
            <xsl:variable name="datePart">
                <xsl:choose>
                    <xsl:when test="starts-with($raw, 'Created') or
                                    starts-with($raw, 'Submitted') or
                                    starts-with($raw, 'Updated')">
                        <xsl:value-of select="substring-after($raw, ' ')"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="$raw"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:variable>

            <xsl:choose>
                <xsl:when test="dc:date and string-length($datePart) &gt;= 4">
                    <xsl:value-of select="substring($datePart, 1, 4)"/>
                </xsl:when>
                <xsl:otherwise>0000</xsl:otherwise>
            </xsl:choose>
        </datacite:publicationYear>
    </xsl:template>
    <!--  ================= RESOURCE TYPE (Mandatory) =================  -->
    <xsl:template name="resourceType">
        <xsl:variable name="typeRaw" select="normalize-space(dc:type[1])"/>
        <xsl:variable name="typeLower" select="translate($typeRaw, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
        <datacite:resourceType>
            <xsl:attribute name="resourceTypeGeneral">
                <xsl:choose>
                    <xsl:when test="contains($typeLower, 'dataset')">Dataset</xsl:when>
                    <xsl:when test="contains($typeLower, 'software')">Software</xsl:when>
                    <xsl:when test="contains($typeLower, 'image')">Image</xsl:when>
                    <xsl:when test="contains($typeLower, 'text') or contains($typeLower, 'article') or contains($typeLower, 'journal') or contains($typeLower, 'book') or contains($typeLower, 'report')">Text</xsl:when>
                    <xsl:when test="contains($typeLower, 'audio') or contains($typeLower, 'sound')">Sound</xsl:when>
                    <xsl:when test="contains($typeLower, 'video') or contains($typeLower, 'film')">Audiovisual</xsl:when>
                    <xsl:when test="contains($typeLower, 'collection')">Collection</xsl:when>
                    <xsl:when test="contains($typeLower, 'event')">Event</xsl:when>
                    <xsl:when test="contains($typeLower, 'model')">Model</xsl:when>
                    <xsl:when test="contains($typeLower, 'service')">Service</xsl:when>
                    <xsl:when test="contains($typeLower, 'workflow')">Workflow</xsl:when>
                    <xsl:otherwise>Other</xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:value-of select="$typeRaw"/>
        </datacite:resourceType>
    </xsl:template>
    <!--  ================= SUBJECTS (Recommended) =================  -->
    <xsl:template name="subjects">
        <xsl:if test="dc:subject">
            <datacite:subjects>
                <xsl:for-each select="dc:subject">
                    <datacite:subject>
                        <xsl:if test="@xml:lang">
                            <xsl:attribute name="xml:lang">
                                <xsl:value-of select="@xml:lang"/>
                            </xsl:attribute>
                        </xsl:if>
                        <xsl:value-of select="normalize-space(.)"/>
                    </datacite:subject>
                </xsl:for-each>
            </datacite:subjects>
        </xsl:if>
    </xsl:template>
    <!--  ================= CONTRIBUTORS (Recommended) =================  -->
    <xsl:template name="contributors">
        <xsl:if test="dc:contributor">
            <datacite:contributors>
                <xsl:for-each select="dc:contributor">
                    <datacite:contributor contributorType="Other">
                        <datacite:contributorName>
                            <xsl:attribute name="nameType">
                                <xsl:choose>
                                    <xsl:when test="contains(., ',')">Personal</xsl:when>
                                    <xsl:otherwise>Organizational</xsl:otherwise>
                                </xsl:choose>
                            </xsl:attribute>
                            <xsl:value-of select="normalize-space(.)"/>
                        </datacite:contributorName>
                    </datacite:contributor>
                </xsl:for-each>
            </datacite:contributors>
        </xsl:if>
    </xsl:template>
    <!--  ================= DATES (Recommended) =================  -->
    <xsl:template name="dates">
        <xsl:if test="dc:date">
            <datacite:dates>
                <xsl:for-each select="dc:date">

                    <!-- normalize input -->
                    <xsl:variable name="raw" select="normalize-space(.)"/>

                    <!-- detect dateType -->
                    <xsl:variable name="type">
                        <xsl:choose>
                            <xsl:when test="starts-with($raw, 'Created')">Created</xsl:when>
                            <xsl:when test="starts-with($raw, 'Submitted')">Submitted</xsl:when>
                            <xsl:when test="starts-with($raw, 'Updated')">Updated</xsl:when>
                            <xsl:otherwise>Issued</xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>

                    <!-- extract ISO date safely -->
                    <xsl:variable name="date">
                        <xsl:choose>
                            <!-- label prefix: take everything after the first space -->
                            <xsl:when test="starts-with($raw, 'Created') or
                                            starts-with($raw, 'Submitted') or
                                            starts-with($raw, 'Updated')">
                                <xsl:value-of select="substring-after($raw, ' ')"/>
                            </xsl:when>

                            <!-- plain ISO date: take first 10 chars -->
                            <xsl:when test="string-length($raw) &gt;= 10">
                                <xsl:value-of select="substring($raw, 1, 10)"/>
                            </xsl:when>

                            <xsl:otherwise/>
                        </xsl:choose>
                    </xsl:variable>

                    <!-- only output valid ISO dates (YYYY-MM-DD) -->
                    <xsl:if test="string-length($date) = 10">
                        <datacite:date>
                            <xsl:attribute name="dateType">
                                <xsl:value-of select="$type"/>
                            </xsl:attribute>
                            <xsl:value-of select="$date"/>
                        </datacite:date>
                    </xsl:if>

                </xsl:for-each>
            </datacite:dates>
        </xsl:if>
    </xsl:template>
    <!--  ================= LANGUAGE (Optional) =================  -->
    <xsl:template name="language">
        <xsl:if test="dc:language">
            <datacite:language>
                <xsl:value-of select="normalize-space(dc:language[1])"/>
            </datacite:language>
        </xsl:if>
    </xsl:template>
    <!--  ================= ALTERNATE IDENTIFIERS (Optional) =================  -->
    <!-- 
        The primary identifier was chosen above (DOI > Handle > URN > first Other).
        Everything else goes here as alternateIdentifiers.
     -->
    <xsl:template name="alternateIdentifiers">
        <xsl:variable name="primaryIsDOI" select="boolean(dc:identifier[contains(., 'doi.org') or contains(., '10.')][1])"/>
        <xsl:variable name="primaryIsHandle" select="boolean(not($primaryIsDOI) and dc:identifier[contains(., 'hdl.handle.net')][1])"/>
        <xsl:variable name="primaryIsURN" select="boolean(not($primaryIsDOI) and not($primaryIsHandle) and dc:identifier[starts-with(., 'urn:')][1])"/>
        <!--  Collect all identifiers that are NOT the one chosen as primary  -->
        <xsl:variable name="hasAlternates">
            <xsl:choose>
                <xsl:when test="$primaryIsDOI">
                    <!--  alternates = non-DOI identifiers + any extra DOIs beyond the first  -->
                    <xsl:if test="dc:identifier[not(contains(., 'doi.org') or contains(., '10.'))] or count(dc:identifier[contains(., 'doi.org') or contains(., '10.')]) > 1">1</xsl:if>
                </xsl:when>
                <xsl:when test="$primaryIsHandle">
                    <xsl:if test="dc:identifier[not(contains(., 'hdl.handle.net'))] or count(dc:identifier[contains(., 'hdl.handle.net')]) > 1">1</xsl:if>
                </xsl:when>
                <xsl:when test="$primaryIsURN">
                    <xsl:if test="dc:identifier[not(starts-with(., 'urn:'))] or count(dc:identifier[starts-with(., 'urn:')]) > 1">1</xsl:if>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:if test="count(dc:identifier) > 1">1</xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:if test="$hasAlternates = '1'">
            <datacite:alternateIdentifiers>
                <xsl:choose>
                    <!--  Primary was a DOI: emit all non-DOI + extra DOIs  -->
                    <xsl:when test="$primaryIsDOI">
                        <xsl:for-each select="dc:identifier[not(contains(., 'doi.org') or contains(., '10.'))]">
                            <datacite:alternateIdentifier>
                                <xsl:attribute name="alternateIdentifierType">
                                    <xsl:call-template name="guessIdentifierType">
                                        <xsl:with-param name="id" select="normalize-space(.)"/>
                                    </xsl:call-template>
                                </xsl:attribute>
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:alternateIdentifier>
                        </xsl:for-each>
                        <xsl:for-each select="dc:identifier[contains(., 'doi.org') or contains(., '10.')][position() > 1]">
                            <datacite:alternateIdentifier alternateIdentifierType="DOI">
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:alternateIdentifier>
                        </xsl:for-each>
                    </xsl:when>
                    <!--  Primary was a Handle: emit all non-Handle + extra Handles  -->
                    <xsl:when test="$primaryIsHandle">
                        <xsl:for-each select="dc:identifier[not(contains(., 'hdl.handle.net'))]">
                            <datacite:alternateIdentifier>
                                <xsl:attribute name="alternateIdentifierType">
                                    <xsl:call-template name="guessIdentifierType">
                                        <xsl:with-param name="id" select="normalize-space(.)"/>
                                    </xsl:call-template>
                                </xsl:attribute>
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:alternateIdentifier>
                        </xsl:for-each>
                        <xsl:for-each select="dc:identifier[contains(., 'hdl.handle.net')][position() > 1]">
                            <datacite:alternateIdentifier alternateIdentifierType="Handle">
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:alternateIdentifier>
                        </xsl:for-each>
                    </xsl:when>
                    <!--  Primary was a URN: emit all non-URN + extra URNs  -->
                    <xsl:when test="$primaryIsURN">
                        <xsl:for-each select="dc:identifier[not(starts-with(., 'urn:'))]">
                            <datacite:alternateIdentifier>
                                <xsl:attribute name="alternateIdentifierType">
                                    <xsl:call-template name="guessIdentifierType">
                                        <xsl:with-param name="id" select="normalize-space(.)"/>
                                    </xsl:call-template>
                                </xsl:attribute>
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:alternateIdentifier>
                        </xsl:for-each>
                        <xsl:for-each select="dc:identifier[starts-with(., 'urn:')][position() > 1]">
                            <datacite:alternateIdentifier alternateIdentifierType="URN">
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:alternateIdentifier>
                        </xsl:for-each>
                    </xsl:when>
                    <!--  Primary was first Other: emit all but the first  -->
                    <xsl:otherwise>
                        <xsl:for-each select="dc:identifier[position() > 1]">
                            <datacite:alternateIdentifier>
                                <xsl:attribute name="alternateIdentifierType">
                                    <xsl:call-template name="guessIdentifierType">
                                        <xsl:with-param name="id" select="normalize-space(.)"/>
                                    </xsl:call-template>
                                </xsl:attribute>
                                <xsl:value-of select="normalize-space(.)"/>
                            </datacite:alternateIdentifier>
                        </xsl:for-each>
                    </xsl:otherwise>
                </xsl:choose>
            </datacite:alternateIdentifiers>
        </xsl:if>
    </xsl:template>
    <!--  Helper: guess identifier type from value  -->
    <xsl:template name="guessIdentifierType">
        <xsl:param name="id"/>
        <xsl:choose>
            <xsl:when test="contains($id, 'doi.org') or contains($id, '10.')">DOI</xsl:when>
            <xsl:when test="contains($id, 'hdl.handle.net')">Handle</xsl:when>
            <xsl:when test="starts-with($id, 'urn:')">URN</xsl:when>
            <xsl:when test="starts-with($id, 'http://') or starts-with($id, 'https://')">URL</xsl:when>
            <xsl:when test="starts-with($id, 'isbn:') or contains($id, 'isbn')">ISBN</xsl:when>
            <xsl:when test="starts-with($id, 'issn:') or contains($id, 'issn')">ISSN</xsl:when>
            <xsl:otherwise>Other</xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <!--  ================= RELATED IDENTIFIERS (Optional) =================  -->
    <xsl:template name="relatedIdentifiers">
        <xsl:if test="dc:relation or dc:source">
            <datacite:relatedIdentifiers>
                <xsl:for-each select="dc:relation">
                    <datacite:relatedIdentifier relationType="References">
                        <xsl:attribute name="relatedIdentifierType">
                            <xsl:call-template name="guessIdentifierType">
                                <xsl:with-param name="id" select="normalize-space(.)"/>
                            </xsl:call-template>
                        </xsl:attribute>
                        <xsl:value-of select="normalize-space(.)"/>
                    </datacite:relatedIdentifier>
                </xsl:for-each>
                <xsl:for-each select="dc:source">
                    <datacite:relatedIdentifier relationType="IsPartOf">
                        <xsl:attribute name="relatedIdentifierType">
                            <xsl:call-template name="guessIdentifierType">
                                <xsl:with-param name="id" select="normalize-space(.)"/>
                            </xsl:call-template>
                        </xsl:attribute>
                        <xsl:value-of select="normalize-space(.)"/>
                    </datacite:relatedIdentifier>
                </xsl:for-each>
            </datacite:relatedIdentifiers>
        </xsl:if>
    </xsl:template>
    <!--  ================= RIGHTS (Optional) =================  -->
    <xsl:template name="rights">
        <xsl:if test="dc:rights">
            <datacite:rightsList>
                <xsl:for-each select="dc:rights">
                    <datacite:rights>
                        <xsl:if test="@xml:lang">
                            <xsl:attribute name="xml:lang">
                                <xsl:value-of select="@xml:lang"/>
                            </xsl:attribute>
                        </xsl:if>
                        <!--  If the rights value looks like a URL, expose it as rightsURI too  -->
                        <xsl:if test="starts-with(normalize-space(.), 'http://') or starts-with(normalize-space(.), 'https://')">
                            <xsl:attribute name="rightsURI">
                                <xsl:value-of select="normalize-space(.)"/>
                            </xsl:attribute>
                        </xsl:if>
                        <xsl:value-of select="normalize-space(.)"/>
                    </datacite:rights>
                </xsl:for-each>
            </datacite:rightsList>
        </xsl:if>
    </xsl:template>
    <!--  ================= DESCRIPTIONS =================  -->
    <xsl:template name="descriptions">
        <xsl:if test="dc:description[normalize-space(.)]">
            <datacite:descriptions>
                <xsl:for-each select="dc:description[normalize-space(.)]">
                    <datacite:description descriptionType="Abstract">
                        <xsl:if test="@xml:lang">
                            <xsl:attribute name="xml:lang">
                                <xsl:value-of select="@xml:lang"/>
                            </xsl:attribute>
                        </xsl:if>
                        <xsl:value-of select="normalize-space(.)"/>
                    </datacite:description>
                </xsl:for-each>
            </datacite:descriptions>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>