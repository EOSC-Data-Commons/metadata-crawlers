<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns="http://datacite.org/schema/kernel-4"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                exclude-result-prefixes="xsl">
  
  <xsl:output method="xml" indent="yes"/>
  
  <xsl:template match="/record">
    
    <oai:record xmlns:oai="http://www.openarchives.org/OAI/2.0/">
      
      <oai:header>
        <oai:identifier>
          <xsl:value-of select="concat(collection_id, ':', gathering_year, ':', species_code, ':', gathering_country_code, ':', translate(normalize-space(gathering_municipality), ' ', '_'))"/>
        </oai:identifier>
        
        <oai:datestamp>
          <xsl:value-of select="last_date_added"/>
        </oai:datestamp>
        
      </oai:header>

      <oai:metadata>
        
        <resource xmlns="http://datacite.org/schema/kernel-4"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xsi:schemaLocation="http://datacite.org/schema/kernel-4
                                      http://schema.datacite.org/meta/kernel-4/metadata.xsd">
        
      
          <!-- Variables -->          
          <xsl:variable name="location">
            <xsl:if test="normalize-space(gathering_municipality)">
              <xsl:value-of select="gathering_municipality"/>
              <xsl:text>, </xsl:text>
            </xsl:if>
            <xsl:value-of select="gathering_country"/>
          </xsl:variable>
          
          
          <!-- DataCite elements -->
          <!-- Identifier -->
          <identifier identifierType="Local">
            <xsl:value-of select="concat(collection_id, ':', gathering_year, ':', species_code, ':', gathering_country_code, ':', translate(normalize-space(gathering_municipality), ' ', '_'))"/>
          </identifier>
          
          
          <!-- Alternate identifier -->
          <xsl:variable name="baseUrl" select="'https://laji.fi/en/observation/list?'" />
          <xsl:variable name="year" select="normalize-space(gathering_year)" />
          <xsl:variable name="locationParam">
            <xsl:choose>
              <xsl:when test="normalize-space(gathering_municipality_code)">
                <xsl:text>finnishMunicipalityId=</xsl:text>
                <xsl:value-of select="gathering_municipality_code"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:text>countryId=</xsl:text>
                <xsl:value-of select="gathering_country_code"/>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:variable>

          <alternateIdentifiers>
            <alternateIdentifier identifierType="URL">
              <xsl:value-of select="$baseUrl"/>
              <xsl:text>collectionId=</xsl:text><xsl:value-of select="collection_id"/>
              <xsl:text>&amp;target=</xsl:text><xsl:value-of select="species_code"/>
              <xsl:text>&amp;time=</xsl:text>
              <xsl:value-of select="$year"/><xsl:text>-01-01%2F</xsl:text><xsl:value-of select="$year"/><xsl:text>-12-31</xsl:text>
              <xsl:text>&amp;</xsl:text><xsl:value-of select="$locationParam"/>
            </alternateIdentifier>
          </alternateIdentifiers>

          
          <!-- Creator -->
          <creators>
            <creator>
              <creatorName><xsl:value-of select="intellectual_owner"/></creatorName>
            </creator>
          </creators>
          
          <!-- Title -->
          <titles>
            <title>
              <xsl:text>Observations of </xsl:text>
              <xsl:value-of select="species_scientific_name"/>
              <xsl:if test="normalize-space(species_english_name)">
                <xsl:text> (</xsl:text>
                <xsl:value-of select="normalize-space(species_english_name)"/>
                <xsl:text>)</xsl:text>
              </xsl:if>
              <xsl:text> from </xsl:text>
              <xsl:value-of select="$location"/>
              <xsl:text> in </xsl:text>
              <xsl:value-of select="gathering_year"/>
            </title>
          </titles>
          
          <!-- Publisher -->
          <publisher><xsl:value-of select="publisher_shortname"/></publisher>
          
          <!-- Publication year -->
          <publicationYear>
            <xsl:value-of select="substring(date_created,1,4)"/>  
          </publicationYear>
          
          <!-- Resource type -->
          <resourceType resourceTypeGeneral="Dataset">Species observation aggregation</resourceType>
          
          <!-- Dates -->
          <dates>
            <!-- Observation time span -->
            <date dateType="Collected" dateInformation="Start of observation period">
              <xsl:value-of select="oldest_record"/>
            </date>
            <date dateType="Collected" dateInformation="End of observation period">
              <xsl:value-of select="newest_record"/>
            </date>
            
            <!-- Collection temporal coverage -->
            <xsl:if test="normalize-space(temporal_coverage)">
              <date dateType="Other" dateInformation="Temporal coverage of the collection">
                <xsl:value-of select="temporal_coverage"/>
              </date>
            </xsl:if>
            
            <!-- Collection metadata lifecycle -->
            <date dateType="Created" dateInformation="Collection metadata">
              <xsl:value-of select="date_created"/>
            </date>
            <date dateType="Updated" dateInformation="Collection metadata">
              <xsl:value-of select="date_edited"/>
            </date>
            
            <!-- Subcollection metadata lifecycle -->
            <date dateType="Created" dateInformation="Record metadata">
              <xsl:value-of select="first_date_added"/>
            </date>
            <date dateType="Updated" dateInformation="Record metadata">
              <xsl:value-of select="last_date_added"/>
            </date>
          </dates>
          
          <!-- Related Identifier -->
          <relatedIdentifiers>
            <relatedIdentifier relatedIdentifierType="Local" relationType="IsPartOf">
              <xsl:value-of select="collection_id"/>
            </relatedIdentifier>
          </relatedIdentifiers>
          
          <!-- Subjects -->
          <subjects>
            <xsl:if test="normalize-space(taxonomic_coverage)">
              <subject><xsl:value-of select="taxonomic_coverage"/></subject>
            </xsl:if>
            <xsl:if test="normalize-space(geographic_coverage)">
              <subject><xsl:value-of select="geographic_coverage"/></subject>
            </xsl:if>
            <subject><xsl:value-of select="gathering_country"/></subject>
            <subject><xsl:value-of select="gathering_country_finnish"/></subject>
            <subject><xsl:value-of select="gathering_municipality"/></subject>
            <subject><xsl:value-of select="species_scientific_name"/></subject>
            <xsl:if test="normalize-space(species_english_name)">
              <subject><xsl:value-of select="species_english_name"/></subject>
            </xsl:if>
            <xsl:if test="normalize-space(species_finnish_name)">
              <subject><xsl:value-of select="species_finnish_name"/></subject>
            </xsl:if>
            <xsl:if test="normalize-space(species_swedish_name)">
              <subject><xsl:value-of select="species_swedish_name"/></subject>
            </xsl:if>
          </subjects>
          
          <!-- Description -->
          <descriptions>
            <description descriptionType="Abstract">
              <xsl:text>This record represents a part of the collection </xsl:text>
              <xsl:value-of select="collection_name"/>
              <xsl:text> of </xsl:text>
              <xsl:value-of select="intellectual_owner"/>
              <xsl:text>, containing </xsl:text>
              <xsl:value-of select="count"/>
              <xsl:text> observations of </xsl:text>
              <xsl:value-of select="species_scientific_name"/>
              <xsl:if test="normalize-space(species_english_name)">
                <xsl:text> (</xsl:text>
                <xsl:value-of select="species_english_name"/>
                <xsl:text>)</xsl:text>
              </xsl:if>
              <xsl:text> in </xsl:text>
              <xsl:value-of select="$location"/>
              <xsl:text> from </xsl:text>
              <xsl:value-of select="gathering_year"/>
              <xsl:text>. Complete collection has </xsl:text>
              <xsl:value-of select="collection_size"/>
              <xsl:text> observations</xsl:text>
              <xsl:if test="normalize-space(taxonomic_coverage)">
                <xsl:text> of </xsl:text>
                <xsl:value-of select="taxonomic_coverage"/>
              </xsl:if>
              <xsl:if test="normalize-space(geographic_coverage)">
                <xsl:text> in </xsl:text>
                <xsl:value-of select="geographic_coverage"/>
              </xsl:if>
              <xsl:if test="normalize-space(temporal_coverage)">
                <xsl:text> during </xsl:text>
                <xsl:value-of select="temporal_coverage"/>
              </xsl:if>
              <xsl:text>.</xsl:text>
            </description>
            
            <xsl:if test="description">
              <description descriptionType="Other">
                <xsl:value-of select="description"/>
              </description>
            </xsl:if>
            
            <xsl:if test="data_quality_description">
              <description descriptionType="TechnicalInfo">
                <xsl:value-of select="data_quality_description"/>
              </description>
            </xsl:if>
            
            <xsl:if test="collection_type">
              <description descriptionType="Other">
                <xsl:text>Collection type: </xsl:text>
                <xsl:value-of select="collection_type"/>
              </description>
            </xsl:if>
          </descriptions>
          
          <!-- Language -->
          <language><xsl:value-of select="language"/></language>
          
          <!-- Geo coverage -->
          <xsl:if test="normalize-space(geographic_coverage)">
            <geoLocations>
              <geoLocation>
                <geoLocationPlace><xsl:value-of select="geographic_coverage"/></geoLocationPlace>
              </geoLocation>
            </geoLocations>
          </xsl:if>
      
      
        </resource>
      </oai:metadata>
    </oai:record>
  </xsl:template>
</xsl:stylesheet>
