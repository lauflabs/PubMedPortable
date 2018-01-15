#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
    Copyright (c) 2014, Bjoern Gruening <bjoern.gruening@gmail.com>, Kersten Doering <kersten.doering@gmail.com>

    This parser reads XML files from PubMed and extracts titles,
    abstracts (no full texts), authors, dates, etc. and directly loads them 
    into the pubmed PostgreSQL database schema (defined in PubMedDB.py).
"""

import sys, os
import xml.etree.cElementTree as etree
import datetime
import warnings
import logging
import time

import PubMedDB
from sqlalchemy.orm import *
from sqlalchemy import *
from sqlalchemy.exc import *
import gzip
from multiprocessing import Pool


WARNING_LEVEL = "always"  # error, ignore, always, default, module, once
# multiple processes, #processors-1 is optimal!
PROCESSES = 4

warnings.simplefilter(WARNING_LEVEL)

# convert 3 letter code of months to digits for unique publication format
month_code = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
              "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}


class FilePreloadScreener:
    def __init__(self, filepath, engine_input):
        Session = sessionmaker(bind=engine_input)
        self.filepath = filepath
        self.session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.flush()
        self.session.close()
        # TODO don't just close the session, close the CONNECTION!
        # TODO docs say this should release connection resources, why isn't it?

    @staticmethod
    def _trim_to_invariant_path(path):
        # Suffix insensitive file name (allow loading with either XML or XML.GZ source)
        return os.path.split(path)[-1]

    def exclude_loaded_files(self, paths):
        parsed_files = self.session \
            .query(PubMedDB.XMLFile.xml_file_name) \
            .all()

        parsed_files_set = set([str(item[0]) for item in parsed_files])

        # Don't load the same file twice - find files still requiring parsing
        unloaded_paths = [p for p in paths if self._trim_to_invariant_path(p) not in parsed_files_set]

        # Tell folks what we're skipping
        for p in paths:
            if p not in unloaded_paths:
                print 'Skipping, file %s already in DB' % (p,)

        print 'Skipping %s files, Parsing %d files' % (len(paths)-len(unloaded_paths), len(unloaded_paths))

        return unloaded_paths


class MedlineParser:

    # db is a global variable and given to MedlineParser(path,db) in _start_parser(path)
    def __init__(self, filepath, engine_input):
        Session = sessionmaker(bind=engine_input)
        self.filepath = filepath
        self.session = Session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.flush()
        self.session.close()

    @staticmethod
    def _limited_string(input_str, limit):
        if input_str is not None:
            if len(input_str) > limit:
                return input_str[0:limit - 2] + '…'
            return input_str
        return input_str

    def _limited_string_lower(self, input_str, limit):
        output_str = self._limited_string(input_str, limit)
        if output_str:
            return output_str.lower()
        return output_str

    def _parse(self):
        _file = self.filepath

        if os.path.splitext(_file)[-1] == ".gz":
            _file = gzip.open(_file, 'rb')

        # get an iterable
        context = etree.iterparse(_file, events=("start", "end"))
        # turn it into an iterator
        context = iter(context)

        # get the root element
        event, root = context.next()

        DBCitation = PubMedDB.Citation()
        db_journal = PubMedDB.Journal()


        DBXMLFile = PubMedDB.XMLFile()
        DBXMLFile.xml_file_name = os.path.split(self.filepath)[-1]
        DBXMLFile.time_processed = datetime.datetime.now()#time.localtime()

        loop_counter = 0  # to check for memory usage each X loops

        for event, elem in context:

            if event == "end":
                if elem.tag == "MedlineCitation" or elem.tag == "BookDocument":
                    loop_counter += 1

                    # catch KeyError in case there is no Owner or Status attribute before committing DBCitation
                    try:
                        DBCitation.citation_owner = elem.attrib["Owner"]
                    except:
                        pass

                    try:
                        DBCitation.citation_status = elem.attrib["Status"]
                    except:
                        pass
                    DBCitation.journals = [db_journal]

                    pubmed_id = int(elem.find("PMID").text)
                    DBCitation.pmid = pubmed_id

                    try:
                        same_pmid = self.session\
                            .query(PubMedDB.Citation.pmid)\
                            .filter(PubMedDB.Citation.pmid == pubmed_id)\
                            .first()
                        # The following condition is only for incremental updates. 

                        """
                        # Implementation that replaces the database entry with the new article from the XML file.
                        if same_pmid: # -> evt. any()
                            same_pmid = same_pmid[0]
                            warnings.warn('\nDoubled Citation found (%s).' % pubmed_id)
                            if not same_pmid.date_revised or same_pmid.date_revised < DBCitation.date_revised:
                                warnings.warn('\nReplace old Citation. Old Citation from %s, new citation from %s.' % (same_pmid.date_revised, DBCitation.date_revised) )
                                self.session.delete( same_pmid )
                                self.session.commit()
                                DBCitation.xml_files = [DBXMLFile] # adds an implicit add()
                                self.session.add( DBCitation )
                        """

                        # Keep database entry that is already saved in database and continue with the next PubMed-ID.
                        # Manually deleting entries is possible (with PGAdmin3 or via command-line), e.g.:
                        # DELETE FROM pubmed.tbl_medline_citation WHERE pmid = 25005691;
                        if same_pmid:
                            print "Article already in database [%s] - Continuing with next PubMed-ID" % (str(same_pmid[0]),)
                            DBCitation = PubMedDB.Citation()
                            db_journal = PubMedDB.Journal()
                            elem.clear()
                            self.session.commit()
                            continue
                        else:
                            DBCitation.xml_files = [DBXMLFile]  # adds an implicit add()
                            self.session.add(DBCitation)

                        # if loop_counter % 100 == 0:
                        # Minimize losses on error/rollback
                        # TODO use larger commit block size once we're got all data problems licked
                        self.session.commit()

                    except IntegrityError as error:
                        warnings.warn("\nIntegrityError: %s, %s, %s" % (self.filepath, pubmed_id, str(error)), Warning)
                        self.session.rollback()
                    except Exception as error:
                        warnings.warn("\nUnknownError: %s, %s, %s" % (self.filepath, pubmed_id, str(error)), Warning)
                        self.session.rollback()

                    DBCitation = PubMedDB.Citation()
                    db_journal = PubMedDB.Journal()
                    elem.clear()

                # Kersten: some dates are given in 3-letter code - use dictionary month_code for conversion to digits:
                if elem.tag == "DateCreated":
                    try:
                        date = datetime.date(int(elem.find("Year").text), int(elem.find("Month").text), int(elem.find("Day").text))
                    except:
                        date = datetime.date(int(elem.find("Year").text), int(month_code[elem.find("Month").text]), int(elem.find("Day").text))
                    DBCitation.date_created = date

                if elem.tag == "DateCompleted":
                    try:
                        date = datetime.date(int(elem.find("Year").text), int(elem.find("Month").text), int(elem.find("Day").text))
                    except:
                        date = datetime.date(int(elem.find("Year").text), int(month_code[elem.find("Month").text]), int(elem.find("Day").text))
                    DBCitation.date_completed = date

                if elem.tag == "DateRevised":
                    try:
                        date = datetime.date(int(elem.find("Year").text), int(elem.find("Month").text), int(elem.find("Day").text))
                    except:
                        date = datetime.date(int(elem.find("Year").text), int(month_code[elem.find("Month").text]), int(elem.find("Day").text))
                    DBCitation.date_revised = date

                if elem.tag == "NumberOfReferences":
                    DBCitation.number_of_references = elem.text

                if elem.tag == "ISSN":
                    db_journal.issn = elem.text
                    db_journal.issn_type = elem.attrib['IssnType']

                if elem.tag == "JournalIssue" or elem.tag == "Book":
                    if elem.find("Volume") != None:         db_journal.volume = elem.find("Volume").text
                    if elem.find("Issue") != None:          db_journal.issue = elem.find("Issue").text

                    # ensure pub_date_year with boolean year:
                    year = False
                    for subelem in elem.find("PubDate"):
                        if subelem.tag == "MedlineDate":
                            db_journal.medline_date = self._limited_string(subelem.text, 40)
                        elif subelem.tag == "Year":
                            year = True
                            db_journal.pub_date_year = subelem.text
                        elif subelem.tag == "Month":
                            if subelem.text in month_code:
                                db_journal.pub_date_month = month_code[subelem.text]
                            else:
                                db_journal.pub_date_month = subelem.text
                        elif subelem.tag == "Day":
                            db_journal.pub_date_day = subelem.text

                    if not year:
                        try:
                            temp_year = db_journal.medline_date[0:4]
                            db_journal.pub_date_year = temp_year
                        except:
                            print _file, " not able to cast first 4 letters of medline_date ", temp_year
                
                
                #if there is the attribute ArticleDate, month and day are given
                if elem.tag == "ArticleDate":
                    db_journal.pub_date_year = elem.find("Year").text
                    db_journal.pub_date_month = elem.find("Month").text
                    db_journal.pub_date_day = elem.find("Day").text

                if elem.tag == "Title":
                    """ ToDo """
                    pass

                if elem.tag == "Journal":
                    if elem.find("Title") != None:
                        db_journal.title = elem.find("Title").text
                    if elem.find("ISOAbbreviation") != None:
                        db_journal.iso_abbreviation = elem.find("ISOAbbreviation").text

                if elem.tag == "ArticleTitle" or elem.tag == "BookTitle":
                    DBCitation.article_title = elem.text
                if elem.tag == "MedlinePgn":
                    DBCitation.medline_pgn = elem.text

                if elem.tag == "AuthorList":
                    #catch KeyError in case there is no CompleteYN attribute before committing DBCitation
                    try:
                        DBCitation.article_author_list_comp_yn = elem.attrib["CompleteYN"]
                    except:
                        pass

                    DBCitation.authors = []
                    for author in elem:
                        db_author = PubMedDB.Author()

                        if author.find("LastName") is not None:
                            db_author.last_name = author.find("LastName").text

                        # Forname is restricted to max 99 characters, but it seems like the None query did not always work - try-except-block
                        try:
                            if author.find("ForeName") is not None:
                                db_author.fore_name = self._limited_string(author.find("ForeName").text, 100)
                        except:
                            pass

                        # Knock down to consistent lowercase for easy lookup
                        if author.find("Initials") is not None:
                            db_author.initials = self._limited_string_lower(author.find("Initials").text, 20)

                        if author.find("Suffix") is not None:
                            db_author.suffix = self._limited_string_lower(author.find("Suffix").text, 20)

                        if author.find("CollectiveName") is not None:
                            db_author.collective_name = author.find("CollectiveName").text

                        DBCitation.authors.append(db_author)

                if elem.tag == "PersonalNameSubjectList":
                    DBCitation.personal_names = []
                    for p_name in elem:
                        db_personal_name = PubMedDB.PersonalName()

                        if p_name.find("LastName") is not None:
                            db_personal_name.last_name = p_name.find("LastName").text

                        if p_name.find("ForeName") is not None:
                            db_personal_name.fore_name = p_name.find("ForeName").text

                        if p_name.find("Initials") is not None:
                            db_personal_name.initials = self._limited_string_lower(p_name.find("Initials").text, 10)

                        if p_name.find("Suffix") is not None:
                            db_personal_name.suffix = p_name.find("Suffix").text

                        DBCitation.personal_names.append(db_personal_name)

                if elem.tag == "InvestigatorList":
                    DBCitation.investigators = []
                    for investigator in elem:
                        db_investigator = PubMedDB.Investigator()

                        if investigator.find("LastName") is not None:
                            db_investigator.last_name = investigator.find("LastName").text

                        if investigator.find("ForeName") is not None:
                            db_investigator.fore_name = investigator.find("ForeName").text

                        if investigator.find("Initials") is not None:
                            db_investigator.initials = investigator.find("Initials").text

                        if investigator.find("Suffix") is not None:
                            db_investigator.suffix = investigator.find("Suffix").text

                        if investigator.find("Affiliation") is not None:
                            db_investigator.investigator_affiliation = investigator.find("Affiliation").text

                        DBCitation.investigators.append(db_investigator)

                if elem.tag == "SpaceFlightMission":
                    DBSpaceFlight = PubMedDB.SpaceFlight()
                    DBSpaceFlight.space_flight_mission = elem.text
                    DBCitation.space_flights = [DBSpaceFlight]

                if elem.tag == "GeneralNote":
                    DBCitation.notes = []
                    for subelem in elem:
                        DBNote = PubMedDB.Note()
                        DBNote.general_note_owner = elem.attrib["Owner"]
                        DBNote.general_note = subelem.text
                        DBCitation.notes.append(DBNote)

                if elem.tag == "ChemicalList":
                    DBCitation.chemicals = []
                    for chemical in elem:
                        DBChemical = PubMedDB.Chemical()

                        if chemical.find("RegistryNumber") is not None:
                            DBChemical.registry_number = chemical.find("RegistryNumber").text

                        if chemical.find("NameOfSubstance") is not None:
                            DBChemical.name_of_substance = chemical.find("NameOfSubstance").text
                            DBChemical.substance_ui = chemical.find("NameOfSubstance").attrib['UI']
                        DBCitation.chemicals.append(DBChemical)

                if elem.tag == "GeneSymbolList":
                    DBCitation.gene_symbols = []
                    for genes in elem:
                        db_gene_symbol = PubMedDB.GeneSymbol()
                        db_gene_symbol.gene_symbol = self._limited_string(genes.text, 40)
                        # TODO is capitalization important here? Normalize?
                        DBCitation.gene_symbols.append(db_gene_symbol)

                if elem.tag == "CommentsCorrectionsList":
                    DBCitation.comments = []
                    for comment in elem:
                        db_comment = PubMedDB.Comment()

                        db_comment.ref_source = self._limited_string(comment.find('RefSource'), 255)
                        db_comment.ref_type = self._limited_string(comment.attrib['RefType'], 22)

                        comment_pmid_version = comment.find('PMID')

                        if comment_pmid_version is not None:
                            db_comment.pmid_version = comment_pmid_version.text

                        DBCitation.comments.append(db_comment)

                if elem.tag == "MedlineJournalInfo":
                    db_journal_info = PubMedDB.JournalInfo()

                    if elem.find("NlmUniqueID") is not None:
                        db_journal_info.nlm_unique_id = elem.find("NlmUniqueID").text

                    if elem.find("Country") is not None:
                        db_journal_info.country = elem.find("Country").text
                    """#MedlineTA is just a name for the journal as an abbreviation
                    Abstract with PubMed-ID 21625393 has no MedlineTA attribute it has to be set in Postgresql, that is why "unknown" is inserted instead. There is just a <MedlineTA/> tag and the same information is given in  </JournalIssue> <Title>Biotechnology and bioprocess engineering : BBE</Title>, but this is not (yet) read in this parser -> line 173:
                    """
                    if elem.find("MedlineTA") is not None and elem.find("MedlineTA").text is None:
                        db_journal_info.medline_ta = "unknown"
                    elif elem.find("MedlineTA") is not None:
                        db_journal_info.medline_ta = elem.find("MedlineTA").text

                    DBCitation.journal_infos = [db_journal_info]

                if elem.tag == "CitationSubset":
                    DBCitation.citation_subsets = []
                    for subelem in elem:
                        DBCitationSubset = CitationSubset(subelem.text)
                        DBCitation.citation_subsets.append(DBCitationSubset)

                if elem.tag == "MeshHeadingList":
                    DBCitation.meshheadings = []
                    DBCitation.qualifiers = []
                    for mesh in elem:
                        DBMeSHHeading = PubMedDB.MeSHHeading()
                        mesh_desc = mesh.find("DescriptorName")
                        if mesh_desc != None:
                            DBMeSHHeading.descriptor_name = mesh_desc.text
                            DBMeSHHeading.descriptor_name_major_yn = mesh_desc.attrib['MajorTopicYN']
                            DBMeSHHeading.descriptor_ui = mesh_desc.attrib['UI']
                        if mesh.find("QualifierName") != None:
                            mesh_quals = mesh.findall("QualifierName")
                            for qual in mesh_quals:
                                DBQualifier = PubMedDB.Qualifier()
                                DBQualifier.descriptor_name = mesh_desc.text
                                DBQualifier.qualifier_name = qual.text
                                DBQualifier.qualifier_name_major_yn = qual.attrib['MajorTopicYN']
                                DBQualifier.qualifier_ui = qual.attrib['UI']
                                DBCitation.qualifiers.append(DBQualifier)
                        DBCitation.meshheadings.append(DBMeSHHeading)

                if elem.tag == "GrantList":
                    #catch KeyError in case there is no CompleteYN attribute before committing DBCitation
                    try:
                        DBCitation.grant_list_complete_yn = elem.attrib["CompleteYN"]
                    except:
                        pass
                    DBCitation.grants = []
                    for grant in elem:
                        DBGrants = PubMedDB.Grant()

                        if grant.find("GrantID") != None:
                            DBGrants.grantid = grant.find("GrantID").text
                        if grant.find("Acronym") != None:
                            DBGrants.acronym = grant.find("Acronym").text
                        if grant.find("Agency") != None:
                            DBGrants.agency = grant.find("Agency").text
                        if grant.find("Country") != None:
                            DBGrants.country = grant.find("Country").text
                        DBCitation.grants.append(DBGrants)

                if elem.tag == "DataBankList":
                    #catch KeyError in case there is no CompleteYN attribute before committing DBCitation
                    try:
                        DBCitation.data_bank_list_complete_yn = elem.attrib["CompleteYN"]
                    except:
                        pass
                    DBCitation.accessions = []
                    DBCitation.databanks = []

                    for databank in elem:
                        DBDataBank = PubMedDB.DataBank()
                        DBDataBank.data_bank_name = databank.find("DataBankName").text
                        DBCitation.databanks.append(DBDataBank)

                        acc_numbers = databank.find("AccessionNumberList")
                        if acc_numbers is not None:
                            for acc_number in acc_numbers:
                                db_accession = PubMedDB.Accession()
                                db_accession.data_bank_name = DBDataBank.data_bank_name
                                db_accession.accession_number = acc_number.text
                                DBCitation.accessions.append(db_accession)

                if elem.tag == "Language":
                    db_language = PubMedDB.Language()
                    db_language.language = elem.text
                    DBCitation.languages = [db_language]

                # TODO many PKEY hits on this, do a check before saving, so we don't lose the article
                if elem.tag == "PublicationTypeList":
                    DBCitation.publication_types = []
                    for subelem in elem:
                        db_publication_type = PubMedDB.PublicationType()
                        db_publication_type.publication_type = subelem.text
                        DBCitation.publication_types.append(db_publication_type)

                if elem.tag == "Article":
                    #ToDo
                    """
                    for subelem in elem:
                        if subelem.tag == "Journal":
                            for sub_subelem in subelem:
                                pass
                        if subelem.tag == "JArticleTitle":
                            pass
                        if subelem.tag == "JPagination":
                            pass
                        if subelem.tag == "JLanguage":
                            pass
                        if subelem.tag == "JPublicationTypeList":
                            pass
                    """

                if elem.tag == "VernacularTitle":
                    DBCitation.vernacular_title = elem.tag

                if elem.tag == "OtherAbstract":
                    db_other_abstract = PubMedDB.OtherAbstract()
                    DBCitation.other_abstracts = []
                    for other in elem:
                        if other.tag == "AbstractText":
                            db_other_abstract.other_abstract = other.text

                    DBCitation.other_abstracts.append(db_other_abstract)

                if elem.tag == "OtherID":
                    DBCitation.other_ids = []
                    db_other_id = PubMedDB.OtherID()

                    db_other_id.other_id = self._limited_string(elem.text, 80)
                    db_other_id.other_id_source = elem.attrib['Source']

                    DBCitation.other_ids.append(db_other_id)

                # start Kersten: some abstracts contain another structure - code changed:
                # check for different labels: "OBJECTIVE", "CASE SUMMARY", ...
                # next 3 lines are unchanged
                if elem.tag == "Abstract":
                    DBAbstract = PubMedDB.Abstract()
                    DBCitation.abstracts = []
                    #prepare empty string for "normal" abstracts or "labelled" abstracts
                    temp_abstract_text = ""
                    #if there are multiple AbstractText-Tags:
                    if elem.find("AbstractText") != None and len(elem.findall("AbstractText")) > 1:
                        for child_AbstractText in elem.getchildren():
                            # iteration over all labels is needed otherwise only "OBJECTIVE" would be pushed into database
                            # debug: check label
                            # [('NlmCategory', 'METHODS'), ('Label', 'CASE SUMMARY')]
                            # ...
                            # also checked for empty child-tags in this structure!
                            if child_AbstractText.tag == "AbstractText" and child_AbstractText.text != None:
                            #if child_AbstractText.tag == "AbstractText": # would give an error!
                                # no label - this case should not happen with multiple AbstractText-Tags:
                                if len(child_AbstractText.items()) == 0:
                                    temp_abstract_text +=child_AbstractText.text + "\n"
                                # one label or the NlmCategory - first index has to be zero:
                                if len(child_AbstractText.items()) == 1:
                                    # filter for the wrong label "UNLABELLED" - usually contains the text "ABSTRACT: - not used: 
                                    if child_AbstractText.items()[0][1] == "UNLABELLED":
                                        temp_abstract_text += child_AbstractText.text + "\n"
                                    else:
                                        temp_abstract_text += child_AbstractText.items()[0][1] + ":\n" + child_AbstractText.text + "\n"
                                # label and NlmCategory - take label - first index has to be one:
                                if len(child_AbstractText.items()) == 2:
                                    temp_abstract_text += child_AbstractText.items()[1][1] + ":\n" + child_AbstractText.text + "\n"    
                    # if there is only one AbstractText-Tag ("usually") - no labels used:
                    if elem.find("AbstractText") != None and len(elem.findall("AbstractText")) == 1:
                        temp_abstract_text = elem.findtext("AbstractText")
                    # append abstract text for later pushing it into db:
                    DBAbstract.abstract_text = temp_abstract_text
                    # next 3 lines are unchanged - some abstract texts (few) contain the child-tag "CopyrightInformation" after all AbstractText-Tags:
                    if elem.find("CopyrightInformation") != None:   
                        DBAbstract.copyright_information = elem.find("CopyrightInformation").text
                    DBCitation.abstracts.append(DBAbstract)
                # end Kersten - code changed
                
                """
                #old code:
                if elem.tag == "Abstract":
                    DBAbstract = PubMedDB.Abstract()
                    DBCitation.abstracts = []

                    if elem.find("AbstractText") != None:   DBAbstract.abstract_text = elem.find("AbstractText").text
                    if elem.find("CopyrightInformation") != None:   DBAbstract.copyright_information = elem.find("CopyrightInformation").text
                    DBCitation.abstracts.append(DBAbstract)
                """
                if elem.tag == "KeywordList":
                    #catch KeyError in case there is no Owner attribute before committing DBCitation
                    try:
                        DBCitation.keyword_list_owner = elem.attrib["Owner"]
                    except:
                        pass
                    DBCitation.keywords = []
                    all_keywords = []
                    for subelem in elem:
                        #some documents contain duplicate keywords which would lead to a key error - if-clause
                        if not subelem.text in all_keywords:
                            all_keywords.append(subelem.text)
                        else:
                            continue
                        DBKeyword = PubMedDB.Keyword()
                        DBKeyword.keyword = subelem.text
                        #catch KeyError in case there is no MajorTopicYN attribute before committing DBCitation
                        try:
                            DBKeyword.keyword_major_yn = subelem.attrib["MajorTopicYN"]
                        except:
                            pass
                        DBCitation.keywords.append(DBKeyword)

                if elem.tag == "Affiliation":
                    DBCitation.article_affiliation = self._limited_string(elem.text, 2000)

                if elem.tag == "SupplMeshList":
                    DBCitation.suppl_mesh_names = []
                    for suppl_mesh in elem:
                        db_suppl_mesh_name = PubMedDB.SupplMeshName()

                        db_suppl_mesh_name.suppl_mesh_name = self._limited_string(suppl_mesh.text, 80)
                        db_suppl_mesh_name.suppl_mesh_name_ui = suppl_mesh.attrib['UI']
                        db_suppl_mesh_name.suppl_mesh_name_type = suppl_mesh.attrib['Type']

                        DBCitation.suppl_mesh_names.append(db_suppl_mesh_name)

        self.session.commit()
        return True


def get_memory_usage(pid=os.getpid(), format="%mem"):
    """
        Get the Memory Usage from a specific process
        @pid = Process ID
        @format = % or kb (%mem or rss) ...
    """
    return float(os.popen('ps -p %d -o %s | tail -1' %
                        (pid, format)).read().strip())


def _start_parser(path, engine_input):
    """
        Used to start MultiProcessor Parsing
    """
    print path, '\tpid:', os.getpid()
    with MedlineParser(path, engine_input) as p:
        p._parse()

    return path


# uses global variable "db" because of result.get()
def run(db_name_input, medline_path, clean, start, end, PROCESSES):
    if end is not None:
        end = int(end)

    # Only make a single database connection pool
    db_engine, base = PubMedDB.init(db_name_input)

    if clean:
        PubMedDB.create_tables(db_engine)

    paths = []
    for root, dirs, files in os.walk(medline_path):
        for filename in files:
            if os.path.splitext(filename)[-1] in [".xml", ".gz"]:
                paths.append(os.path.join(root, filename))

    # Don't reload what we've already got
    with FilePreloadScreener(paths, db_engine) as screener:
        paths = screener.exclude_loaded_files(paths)

    paths.sort()

    print "Running for %d files" % (len(paths),)

    # result.get() needs global variable `db` now - that is why a line "db = options.database" is added in "__main__" -
    #  the variable db cannot be given to __start_parser in map_async()

    if PROCESSES > 1 and len(paths) > 1:

        from functools import partial

        with Pool(processes=PROCESSES) as pool:
            print "Running multi-process with %d processes" % (PROCESSES,)
            result = pool.map_async(
                partial(_start_parser, db_engine=db_engine),
                paths[start:end]
            )

            res = result.get()

    # without multiprocessing:
    else:
        print "Running single process"
        for path in paths:
            _start_parser(path, db_engine)

    print "######################"
    print "###### Finished ######"
    print "######################"


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-c", "--no_cleaning", dest="clean",
                      action="store_false", default=True,
                      help="Truncate the Database before running the parser (default: True).")
    parser.add_option("-s", "--start",
                      dest="start", default=0,
                      help="All queued files are passed if no start and end parameter is set. Otherwise you can specify a start and end o the queue. For example to split the parsing on several machines.")
    parser.add_option("-e", "--end",
                      dest="end", default=None,
                      help="All queued files are passed if no start and end parameter is set. Otherwise you can specify a start and end o the queue. For example to split the parsing on several machines.")
    parser.add_option("-i", "--input", dest="medline_path",
                      default='data/pancreatic_cancer/',
                      help="specify the path to the medine XML-Files (default: data/pancreatic_cancer/)")
    parser.add_option("-p", "--processes",
                      dest="PROCESSES", default=2,
                      help="How many processes should be used. (Default: 2)")
    parser.add_option("-d", "--database",
                      dest="database", default="pancreatic_cancer_db",
                      help="What is the name of the database. (Default: pancreatic_cancer_db)")

    (options, args) = parser.parse_args()
    db_name = options.database
    #log start time of programme:
    start = time.asctime()
    run(db_name, options.medline_path, options.clean, int(options.start), options.end, int(options.PROCESSES))
    #end time programme 
    end = time.asctime()

    print "programme started - " + start
    print "programme ended - " + end
