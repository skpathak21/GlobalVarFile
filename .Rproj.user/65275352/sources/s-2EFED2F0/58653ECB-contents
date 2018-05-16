#' @title Generate PMML
#'
#' @description This package will Generate PMML
#'
#' @param Symbol
#'
#' @return NULL
#'
#' @examples GeneratePmml("Name,Age,Salary", "String,String,String", "Print()")
#'
#' @export

GeneratePmml <- function(colnames, coltypes, script)
{
  print("ttete")
  xmldoc <-
    read_xml(
      "<Action Id='6254f8f0-73d2-48e0-ad0d-f4b804f90513' NextId='' PreviousId=''><Type>Script</Type><Name>Python</Name><Property Name='SelectedColumns'>Duration_ms</Property><Property Name='PredictedName'>test</Property><Property Name='PredictedDataType'>String</Property><Property Name='ApplyTransformationTo'><![CDATA[exec ('from DataFrameOperation import DataframeOpr \ndfs = DataframeOpr.PerformLoadData('', 5,'6121a56cb86f45998c0ede3940ff30c7_1', 'http://localhost:56620/RESTService.svc/','9F9AAAA2-5398-4557-9D0E-8CD85203093D')DataframeOpr.FinalOutput(dfs['DiagnosticData'])')]]></Property><UUID>ec095401a6784140b0158707d9cddd3f</UUID><ExperimentId>9F9AAAA2-5398-4557-9D0E-8CD85203093D</ExperimentId><ServiceUrl>http://34.216.98.159:8085/MTS/RESTService.svc</ServiceUrl><NodeId>6254f8f0-73d2-48e0-ad0d-f4b804f90513</NodeId></Action>"
    )
  Properties <- xml_text(xml_find_all(xmldoc, ".//Property"))
  selectedcolumns <- Properties[1]
  predictedname <- Properties[2]
  predicteddatatype <- Properties[3]
  execscript <- Properties[4]
  execscriptContent <-
    strsplit(execscript, "(\\r\\n?+|.\\n)", perl = TRUE)[[1]]

  for (execscriptContentItem in execscriptContent)
  {
    if ("DataframeOpr" == execscriptContentItem ||
        'dfs["' == execscriptContentItem)
    {
      execscript <- execscript.replace(execscriptContentItem, "")
    }
  }
  PmmlTag(colnames)
  fullschema <- ''
  fullschema <- paste0(fullschema, startpmmltag)
  fullschema <-
    paste0(fullschema, CreateHeader('python script', 'Pangea'))
  print(fullschema)
  fullschema <- paste0(fullschema, starttranformationtag)
  fullschema <-
    paste0(
      fullschema,
      CreateDerivedFunction(
        funcname = 'pangeacommand.PythonEngine.ScriptExecutor.execute',
        datatype = 'float',
        optype = 'continuous',
        paramname = 'params',
        paramoptype = 'continuous',
        paramdatatype = 'float'

      )
    )
  print(fullschema)
  fullschema <-
    paste0(
      fullschema,
      SetDerivedField("pangeacommand.PythonEngine.ScriptExecutor.execute",
                      selectedcolumns,
                      predictedname,
                      predicteddatatype,
                      paste0("<![CDATA['" ,
                             execscript ,
                             "']]>")
      )
    )
  fullschema <- paste0(fullschema, endtranformationtag)
  fullschema <- paste0(fullschema, endpmmltag)
  print(fullschema)
  return(fullschema)

}

SaveFileToHdfs <- function(writeFolderPath,
                           keys,
                           values,
                           inputscript)
{
  HadoopHost <- hdfsurl
  HDFSPort <- hdfsport
  WebHDFSPort <- wehdfsport
  #client_hdfs = InsecureClient("http://" + HadoopHost + ":" + WebHDFSPort)
  client_hdfs = paste0("http://" , HadoopHost , ":" , WebHDFSPort, "/webhdfs/v1")

  pmmlxml<-GeneratePmml(keys, values, inputscript)
  buf <- charToRaw(pmmlxml)
  #buf <- io.BytesIO(GeneratePmml(keys, values, inputscript))
  filename <- paste0(writeFolderPath , "/pmml/" , "part-00000")
  print("============File name for PMML==========")
  print(filename)

  #other Ways


  modelfile <- hdfs.file(filename, "w")

  #data1 <- toJSON(pmmlxml)

  pmmlxmlbyte <- charToRaw(pmmlxml)

  hdfs.write(pmmlxmlbyte,modelfile)

  hdfs.close(modelfile)

  #End other way



  optionnalParameters <- "&overwrite=true"

  # CREATE => creation of a file
  writeParameter <- "?op=CREATE"

  # Concatenate all the parameters into one uri
  uri <-
    paste0(client_hdfs, filename, writeParameter, optionnalParameters)

  # Ask the namenode on which datanode to write the file
  response <- PUT(uri)

  # Get the url of the datanode returned by hdfs
  uriWrite <- response$url
  if (!file.exists(filename)) {
    responseWrite <- PUT(uriWrite, body = upload_file(filename))
    #hdfs.write(buf,filename)

  }

}

PmmlTag <- function(columns)
{
  startdictionarytag <<- ''
  enddictionarytag <<- ''
  startpmmltag <<- ''
  starttranformationtag <<- ''
  endtranformationtag <<- ''
  startpmmltag <<-'<?xml version="1.0" encoding="UTF-8"?><PMML xmlns="http://www.dmg.org/PMML-4_2" version="4.2">'
  endpmmltag <<- ''
  endpmmltag <<- '</PMML>'
  starttranformationtag <<- '<TransformationDictionary>'
  endtranformationtag <<- '</TransformationDictionary>'
  if ("" != columns)
  {
    splitresult <- strsplit(columns, ',')[[1]]
    startdictionarytag <<-paste0('<DataDictionary numberOfFields="' ,toString(length(splitresult)) ,'">')
    enddictionarytag <<- '</DataDictionary>'
  }
}

CreateHeader <- function(description, appname)
{
  print("Header print")
  headertext <-
    paste0(
      '<Header description="',
      description,
      '"><Application name="',
      appname ,
      '"/><Timestamp>' ,
      toString(Sys.time()) ,
      '</Timestamp></Header>'
    )
  print(headertext)
  return(headertext)
}

CreateDerivedFunction <-
  function(funcname,
           datatype,
           optype,
           paramname,
           paramoptype,
           paramdatatype)
  {
    print("Step3")
    strvalue = ''
    definestarttag <-
      paste0(
        '<DefineFunction name="',
        funcname,
        '" dataType="',
        datatype ,
        '" optype="',
        optype ,
        '">'
      )
    paramfieldtag <-
      paste0(
        '<ParameterField name="',
        paramname ,
        '" optype="' ,
        paramoptype ,
        '" dataType="' ,
        paramdatatype ,
        '"/>'
      )
    discretizestarttag <-
      paste0(
        '<Discretize field="' ,
        paramname ,
        '" ',
        'defaultValue="args,argTypes,argValues">',
        '<DiscretizeBin binValue="df,script,outparam">' ,
        '<Interval closure="openClosed"/>' ,
        '</DiscretizeBin><DiscretizeBin binValue="string,PythonCode,string">',
        '<Interval closure="openClosed"/>',
        '</DiscretizeBin></Discretize>'
      )
    defineendtag <- '</DefineFunction>'
    strvalue = paste(strvalue, definestarttag)
    strvalue = paste(strvalue, paramfieldtag)
    strvalue = paste(strvalue, discretizestarttag)
    strvalue = paste(strvalue, defineendtag)
    print("Step4")
    print(strvalue)
    return(strvalue)
  }

SetDerivedField <-
  function(funcname,
           selectedcolumns,
           predictedname,
           predicteddatatype,cdata)
  {
    print(selectedcolumns)
    print(predictedname)
    print(predicteddatatype)
    datafield = ''
    if ('' != selectedcolumns &&
        '' != predictedname && '' != predicteddatatype)
    {
      datafield <-
        paste0(
          datafield,
          '<DerivedField dataType="' ,
          predicteddatatype ,
          '" name="',
          predictedname,
          '" optype="continuous">'
        )
      datafield <-
        paste0(datafield,
               CreateApplyFunc(funcname, selectedcolumns, cdata))
      datafield <- paste0(datafield, "</DerivedField>")
      return(datafield)
    }
  }

CreateApplyFunc <- function(funcname, fieldnames, cdata)
{
  applystarttag <- paste0("<Apply function='",funcname , "'>")
  applyendtag <- "</Apply>"
  extentionstart <- paste0("<Extension><Script>",cdata , "</Script></Extension>")
  colnameslst <- strsplit(fieldnames, ',')[[1]]
  fieldsstr = ''
  if (length(colnameslst) > 0)
  {
    for (index in seq_along(colnameslst)) {
      fieldsstr <-
        paste0(fieldsstr, "<FieldRef field='" , colnameslst[index], "'/>")
      applystarttag <- paste0(applystarttag, extentionstart)
      applystarttag <- paste0(applystarttag, fieldsstr)
      applystarttag <- paste0(applystarttag, applyendtag)

      return(applystarttag)
    }

  }
}
