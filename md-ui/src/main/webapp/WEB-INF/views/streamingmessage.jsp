<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="security"
	   uri="http://www.springframework.org/security/tags" %>
<%@taglib uri="http://www.springframework.org/tags" prefix="spring"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>

	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
		<script src="../js/jquery.min.js"></script>
		<link href="../css/jquery-ui-1.10.3.custom.css" rel="stylesheet">
		<link href="../css/css/bootstrap.min.css" rel="stylesheet" />
		<script src="../js/jquery-ui-1.10.3.custom.js"></script>
		<script src="../js/jquery.steps.min.js"></script>
		<link rel="stylesheet" href="../css/jquery.steps.css" />
		<link rel="stylesheet" href="../css/jquery.steps.custom.css" />
		<link href="../css/bootstrap.custom.css" rel="stylesheet" />
		<script src="../js/bootstrap.js" type="text/javascript"></script>
		<script src="../js/jquery.jtable.js" type="text/javascript"></script>
		<script src="../js/angular.min.js" type="text/javascript"></script>
		<link href="../css/jtables-bdre.css" rel="stylesheet" type="text/css" />
		<script >
                function fetchPipelineInfo(pid){
        			location.href = '<c:url value="/pages/lineage.page?pid="/>' + pid;
                }
        		</script >
		<script>

var jsonObj = {"Result":"OK","Records":[],"Message":null,"TotalRecordCount":0,"Record":[]};
var map = new Object();
var createJobResult;
var requiredProperties;
var sourceFlag;
var created = 0;

var getGenConfigMap = function(cfgGrp){
    var map = new Object();
    $.ajax({
        type: "GET",
        url: "/mdrest/genconfig/"+cfgGrp+"/?required=2",
        dataType: 'json',
        async: false,
        success: function(data) {

            var root = 'Records';
            $.each(data[root], function(i, v) {
                map[v.key] = v;
            });

        },
        error : function(data){
            console.log(data);
        }

    });
return map;

};

		</script>


		<script type="text/javascript">
function addDataToJson(properties) {
	console.log(properties);
	var id = properties.id;
	console.log(id);
}

		</script>
			<script type="text/javascript">
        		function copyForm() {
        		    var newform2=$('#rawTableColumn').clone(); //Clone form 1
                    newform2.filter('form').prop('id', 'baseTableColumn'); //Update formID
                    newform2.children("#rawDescription").children("#rawFormGroup1").attr('id', 'baseFormGroup1');
                    newform2.children("#rawDescription").children("#rawFormGroup1").children('#rawColName.1').attr('id', 'baseColName.1');
                    newform2.children("#rawDescription").children("#rawFormGroup1").children('#rawDataType.1').attr('id', 'baseDataType.1');
                    newform2.children("#rawDescription").children("#rawFormGroup1").children('#rawRemove1').attr('id', 'baseRemove1');
                    newform2.children("#rawDeleteDiv").attr('id', 'baseDeleteDiv');
                    newform2.children("#baseDeleteDiv").children("#rawButton1").attr('id', 'baseButton1');
                    $('#baseTableColumn').replaceWith(newform2);
        		}
        		</script>

		</script>
		<script>
var wizard = null;
var finalJson;
wizard = $(document).ready(function() {

	$("#bdre-data-load").steps({
		headerTag: "h3",
		bodyTag: "section",
		transitionEffect: "slideLeft",
		stepsOrientation: "vertical",
		enableCancelButton: true,
		onStepChanging: function(event, currentIndex, newIndex) {
			console.log(currentIndex + 'current ' + newIndex );
			if(currentIndex == 0 && newIndex == 1) {
			console.log(document.getElementById('fileFormat').elements[1].value);
			      if(document.getElementById('fileFormat')!=null)
			    buildForm(document.getElementById('fileFormat').elements[1].value);
			}
			return true;
		},
		onStepChanged: function(event, currentIndex, priorIndex) {
			console.log(currentIndex + " " + priorIndex);
			if(currentIndex == 2 && priorIndex == 1) {
				{

					formIntoMap('fileformat_', 'fileFormat');
					jtableIntoMap('rawtablecolumn_', 'rawTableColumnDetails');

					$('#createjobs').on('click', function(e) {

						$.ajax({
							type: "POST",
							url: "/mdrest/message/createjobs",
							data: jQuery.param(map),
							success: function(data) {
								if(data.Result == "OK") {
									created = 1;
									$("#div-dialog-warning").dialog({
										title: "",
										resizable: false,
										height: 'auto',
										modal: true,
										buttons: {
											"Ok": function() {
												$(this).dialog("close");
											}
										}
									}).html('<p><span class="jtable-confirm-message"><spring:message code="dataload.page.success_msg"/></span></p>');
								}
							}

						});
                    return false;
					});

				}
			}
		},
		onFinished: function(event, currentIndex) {
			if(created == 1) {
				location.href = '<c:url value="/pages/process.page"/>';
			} else {
				$("#div-dialog-warning").dialog({
					title: "",
					resizable: false,
					height: 'auto',
					modal: true,
					buttons: {
						"Ok": function() {
							$(this).dialog("close");
						}
					}
				}).html('<p><span class="jtable-confirm-message"><spring:message code="dataload.page.failed_msg"/></span></p>');
			}
		},
		onCanceled: function(event) {
			location.href = '<c:url value="/pages/dataload.page"/>';
		}
	});
});

		</script>
		<script type="text/javascript">
            $(document).ready(function(){
            var next = 1;
            $(".add-more").click(function(e){
                e.preventDefault();
                var addto = "#rawDeleteDiv";
                var addRemove = "#rawFormGroup" + (next);
                next = next + 1;
                var removeBtn = '<button id="rawRemove' + (next) + '" class="btn btn-danger remove-me" ><span class="glyphicon glyphicon-trash" ></span></button></div><div id="field">';
                var newIn = '';
                newIn = newIn +  '<div class="form-group" id="rawFormGroup' + next + '">' ;
                newIn = newIn +  '<div class="col-md-3">' ;
                newIn = newIn +  '<input type="text" class="form-control input-sm" id="rawColName.' + next + '" value="" name="rawColName.' + next + '" placeholder="Column Name" />' ;
                newIn = newIn +  '</div>' ;
                newIn = newIn +  '<div class="col-md-3">' ;
                newIn = newIn +  '<input type="text" class="form-control input-sm" id=rawDataType.' + next + '" value="" name="rawDataType.' + next + '" placeholder="Data Type" />' ;
                newIn = newIn +  '</div>' ;
                newIn = newIn + removeBtn;
                newIn = newIn +  '</div>' ;

                var newInput = $(newIn);
                var removeButton = $(removeBtn);
                $(addto).before(newInput);

                $("#formGroup" + next).attr('data-source',$(addto).attr('data-source'));
                $("#count").val(next);

                    $('.remove-me').click(function(e){
                        e.preventDefault();
                        var fieldNum = this.id.charAt(this.id.length-1);
                        var fieldID = "#rawFormGroup" + fieldNum;
                        console.log($(this));
                        //$(this).remove();
                        $(fieldID).remove();
                    });
            });
        });
        </script>
        	<script type="text/javascript">
                    $(document).ready(function(){
                    var next = 1;
                    $(".add-more").click(function(e){
                        e.preventDefault();
                        var addto = "#baseDeleteDiv";
                        var addRemove = "#baseFormGroup" + (next);
                        next = next + 1;
                        var removeBtn = '<button id="rawRemove' + (next) + '" class="btn btn-danger remove-me" ><span class="glyphicon glyphicon-trash" ></span></button></div><div id="field">';
                        var newIn = '';
                        newIn = newIn +  '<div class="form-group" id="baseFormGroup' + next + '">' ;
                        newIn = newIn +  '<div class="col-md-3">' ;
                        newIn = newIn +  '<input type="text" class="form-control input-sm" id="baseColName.' + next + '" value="" name="baseColName.' + next + '" placeholder="Column Name" />' ;
                        newIn = newIn +  '</div>' ;
                        newIn = newIn +  '<div class="col-md-3">' ;
                        newIn = newIn +  '<input type="text" class="form-control input-sm" id=baseDataType.' + next + '" value="" name="baseDataType.' + next + '" placeholder="Data Type" />' ;
                        newIn = newIn +  '</div>' ;
                        newIn = newIn + removeBtn;
                        newIn = newIn +  '</div>' ;

                        var newInput = $(newIn);
                        var removeButton = $(removeBtn);
                        $(addto).before(newInput);

                        $("#formGroup" + next).attr('data-source',$(addto).attr('data-source'));
                        $("#count").val(next);

                            $('.remove-me').click(function(e){
                                e.preventDefault();
                                var fieldNum = this.id.charAt(this.id.length-1);
                                var fieldID = "#baseFormGroup" + fieldNum;
                                console.log($(this));
                                //$(this).remove();
                                $(fieldID).remove();
                            });
                    });
                });
                </script>
        <script type="text/javascript">
                    $(document).ready(function(){
                    var next = 1;
                    $(".add-more").click(function(e){
                        e.preventDefault();
                        var addto = "#serdePropDiv";
                        var addRemove = "#formGroupSerde" + (next);
                        next = next + 1;
                        var removeBtn = '<button id="removeserde' + (next) + '" class="btn btn-danger remove-me" ><span class="glyphicon glyphicon-trash" ></span></button></div><div id="field">';
                        var newIn = '';
                        newIn = newIn +  '<div class="form-group" id="formGroupSerde' + next + '">' ;
                        newIn = newIn +  '<div class="col-md-3">' ;
                        newIn = newIn +  '<input type="text" class="form-control input-sm" id="serdePropKey.' + next + '" value="" name="serdePropKey.' + next + '" placeholder="Serde Key" />' ;
                        newIn = newIn +  '</div>' ;
                        newIn = newIn +  '<div class="col-md-3">' ;
                        newIn = newIn +  '<input type="text" class="form-control input-sm" id="serdePropValue.' + next + '" value="" name="serdePropValue.' + next + '" placeholder="Serde Property" />' ;
                        newIn = newIn +  '</div>' ;
                        newIn = newIn + removeBtn;
                        newIn = newIn +  '</div>' ;

                        var newInput = $(newIn);
                        var removeButton = $(removeBtn);
                        $(addto).before(newInput);

                        $("#formGroupSerde" + next).attr('data-source',$(addto).attr('data-source'));
                        $("#count").val(next);

                            $('.remove-me').click(function(e){
                                e.preventDefault();
                                var fieldNum = this.id.charAt(this.id.length-1);
                                var fieldID = "#formGroupSerde" + fieldNum;
                                console.log($(this));
                                //$(this).remove();
                                $(fieldID).remove();
                            });
                    });
                });
                </script>
                      <script type="text/javascript">
                                    $(document).ready(function(){
                                    var next = 1;
                                    $(".add-more").click(function(e){
                                        e.preventDefault();
                                        var addto = "#tablePropDiv";
                                        var addRemove = "#formGroupTable" + (next);
                                        next = next + 1;
                                        var removeBtn = '<button id="removetable' + (next) + '" class="btn btn-danger remove-me" ><span class="glyphicon glyphicon-trash" ></span></button></div><div id="field">';
                                        var newIn = '';
                                        newIn = newIn +  '<div class="form-group" id="formGroupTable' + next + '">' ;
                                        newIn = newIn +  '<div class="col-md-3">' ;
                                        newIn = newIn +  '<input type="text" class="form-control input-sm" id="tablePropKey.' + next + '" value="" name="tablePropKey.' + next + '" placeholder="Table Prop Key" />' ;
                                        newIn = newIn +  '</div>' ;
                                        newIn = newIn +  '<div class="col-md-3">' ;
                                        newIn = newIn +  '<input type="text" class="form-control input-sm" id="tablePropValue.' + next + '" value="" name="tablePropValue.' + next + '" placeholder="Table Property" />' ;
                                        newIn = newIn +  '</div>' ;
                                        newIn = newIn + removeBtn;
                                        newIn = newIn +  '</div>' ;

                                        var newInput = $(newIn);
                                        var removeButton = $(removeBtn);
                                        $(addto).before(newInput);

                                        $("#formGroupTable" + next).attr('data-source',$(addto).attr('data-source'));
                                        $("#count").val(next);

                                            $('.remove-me').click(function(e){
                                                e.preventDefault();
                                                var fieldNum = this.id.charAt(this.id.length-1);
                                                var fieldID = "#formGroupTable" + fieldNum;
                                                console.log($(this));
                                                //$(this).remove();
                                                $(fieldID).remove();
                                            });
                                    });
                                });
                                </script>
		<script>
                var app = angular.module('myApp', []);
                app.controller('myCtrl', function($scope) {
                    $scope.fileformats= getGenConfigMap('file_format');
                    $scope.formatMap=null;
                    $scope.busDomains = {};
                    $.ajax({
                    url: '/mdrest/busdomain/options/',
                        type: 'POST',
                        dataType: 'json',
                        async: false,
                        success: function (data) {
                            $scope.busDomains = data;
                        },
                        error: function () {
                            alert('danger');
                        }
                    });


                    $scope.workflowTypes = {};
                    $.ajax({
                    url: '/mdrest/workflowtype/optionslist',
                        type: 'POST',
                        dataType: 'json',
                        async: false,
                        success: function (data) {
                            $scope.workflowTypes = data;
                        },
                        error: function () {
                            alert('danger');
                        }
                    });
                });
        </script>





	</head>
<body ng-app="myApp" ng-controller="myCtrl" >
	<div class="page-header"><spring:message code="dataload.page.panel_heading"/></div>
	<div class="alert alert-info" role="alert">
		<spring:message code="dataload.page.alert_info_outer_heading" />
	</div>
	<div id="bdre-data-load" ng-controller="myCtrl">




			<h3><div class="number-circular">1</div><spring:message code="dataload.page.h3_div_2"/></h3>
            			<section>
            			 <div class="alert alert-info" role="alert">
                                          <spring:message code="dataload.page.alert_info_form"/>
                                        </div>
            <form class="form-horizontal" role="form" id="fileFormat">


                                        <!-- btn-group -->
                                        <div id="rawTablDetailsDB">
                                        <div class="form-group" >
                                            <label class="control-label col-sm-2" for="messageName">Message Name</label>
                                            <div class="col-sm-10">
                                                <input type="text" class="form-control"  id="messageName" name="messageName" placeholder="message name" value="" required>
                                            </div>
                                        </div>
                                        <div class="form-group">
                                            <label class="control-label col-sm-2" for="fileformat"><spring:message code="dataload.page.file_format"/></label>
                                            <div class="col-sm-10">
                                                <select class="form-control" id="fileformat" name="fileformat" >
                                                    <option ng-repeat="fileformat in fileformats" value="{{fileformat.defaultVal}}" name="fileformat">{{fileformat.value}}</option>

                                                </select>
                                            </div>
                                        </div>
                                        <div class="clearfix"></div>
                                        </div>

                                        <!-- /btn-group -->

                                    </form>
            			</section>
			<h3><div class="number-circular">2</div><spring:message code="dataload.page.raw_table_props"/></h3>
			<section>
			    <div id="rawTableColumnDetails"></div>
			    </section>
			<h3><div class="number-circular">3</div><spring:message code="dataload.page.confirm"/></h3>
			<section>
				<div id="Process">
					<button id="createjobs" type="button" class="btn btn-primary btn-lg"><spring:message code="dataload.page.create_jobs"/></button>
				</div>
			</section>
		</div>

		<div style="display:none" id="div-dialog-warning">
			<p><span class="ui-icon ui-icon-alert" style="float:left;"></span></p>
		</div>

		<script type="text/javascript">
	$(document).ready(function () {
	$('#rawTableColumnDetails').jtable({
		title: '<spring:message code="dataload.page.title_raw_table"/>',
		paging: false,
		sorting: false,
		create: false,
		edit: false,
		actions: {
			listAction: function(postData, jtParams) {
				return jsonObj;
			},
			createAction: function(postData) {
                console.log(postData);
                var serialnumber = 1;
                var rawSplitedPostData = postData.split("&");
                var rawJSONedPostData = '{';
                rawJSONedPostData += '"serialNumber":"';
                rawJSONedPostData += serialnumber;
                serialnumber += 1;
                rawJSONedPostData += '"';
                rawJSONedPostData += ',';
                for (i=0; i < rawSplitedPostData.length ; i++)
                {
                    console.log("data is " + rawSplitedPostData[i]);
                    rawJSONedPostData += '"';
                    rawJSONedPostData += rawSplitedPostData[i].split("=")[0];
                    rawJSONedPostData += '"';
                    rawJSONedPostData += ":";
                    rawJSONedPostData += '"';
                    rawJSONedPostData += rawSplitedPostData[i].split("=")[1];
                    rawJSONedPostData += '"';
                    rawJSONedPostData += ',';
                    console.log("json is" + rawJSONedPostData);
                }
                var rawLastIndex = rawJSONedPostData.lastIndexOf(",");
                rawJSONedPostData = rawJSONedPostData.substring(0,rawLastIndex);
                rawJSONedPostData +=  "}";
                console.log(rawJSONedPostData);


               var rawReturnObj='{"Result":"OK","Record":' + rawJSONedPostData + '}';
               var rawJSONedReturn = $.parseJSON(rawReturnObj);

               return $.Deferred(function($dfd) {
                                console.log(rawJSONedReturn);
                                $dfd.resolve(rawJSONedReturn);
                            });

				},

			updateAction: function(postData) {

				return $.Deferred(function($dfd) {
					console.log(postData);
					$dfd.resolve(jsonObj);
				});
			},
			deleteAction: function(item) {
				console.log(item.key);
				return $.Deferred(function($dfd) {
					$dfd.resolve(jsonObj);
				});
			}

		},
		fields: {
		    serialNumber:{
		        key : true,
		        list:false,
		        create : false,
		        edit:false
		    },

			columnName: {
				title: '<spring:message code="dataload.page.title_col_name"/>',
				width: '50%',
				edit: true,
				create:true
			},
			dataType: {

				create: true,
				title: 'Data Type',
				edit: true,
				options:{ 'BigInt':'BigInt',
                          'SmallInt':'SmallInt',
                          'Float':'Float',
                          'Double':'Double',
                          'Decimal':'Decimal',
                          'Timestamp':'Timestamp',
                          'Date':'Date',
                          'String':'String'}
			}
		}

	});



	$('#rawTableColumnDetails').jtable('load');

});

		</script>



		<script>
function buildForm(fileformat) {
	console.log('inside the function');

	$.ajax({
		type: "GET",
		url: "/mdrest/genconfig/" + fileformat + "/?required=2",
		dataType: 'json',
		success: function(data) {
			var root = 'Records';
			var div = document.getElementById('fileFormatDetails');
			var formHTML = '';
			formHTML = formHTML + '<div class="alert alert-info" role="alert"><spring:message code="dataload.page.form_alert_msg"/></div>';
			formHTML = formHTML + '<div id="Serde, OutPut and Input Format">';
			formHTML = formHTML + '<form class="form-horizontal" role="form" id = "formatFields">';



			console.log(data[root].length);
			if (data[root].length == 0){

			        formHTML = formHTML + '<div class="form-group"> <label class="control-label col-sm-3" for="inputFormat">Input Format:</label>';
                    formHTML = formHTML + '<div class="col-sm-9">';
                    formHTML = formHTML + '<input name="inputFormat" value="" placeholder="input format to be used" type="text" class="form-control" id="inputFormat"></div>';
                    formHTML = formHTML + '</div>';
                    formHTML = formHTML + '<div class="form-group"> <label class="control-label col-sm-3" for="outputFormat">Output Format:</label>';
                    formHTML = formHTML + '<div class="col-sm-9">';
                    formHTML = formHTML + '<input name="outputFormat" value="" placeholder="output format to be used" type="text" class="form-control" id="outputFormat"></div>';
                    formHTML = formHTML + '</div>';
                    formHTML = formHTML + '<div class="form-group"> <label class="control-label col-sm-3" for="serdeClass">Serde Class:</label>';
                    formHTML = formHTML + '<div class="col-sm-9">';
                    formHTML = formHTML + '<input name="serdeClass" value="" placeholder="serde class to be used" type="text" class="form-control" id="serdeClass"></div>';
                    formHTML = formHTML + '</div>';

			}else{
			$.each(data[root], function(i, v) {
				formHTML = formHTML + '<div class="form-group"> <label class="control-label col-sm-3" for="' + v.key + '">' + v.value +':</label>';
				formHTML = formHTML + '<div class="col-sm-9">';
				formHTML = formHTML + '<input name="' + v.key + '" value="' + v.defaultVal + '" placeholder="' + v.description + '" type="' + v.type + '" class="form-control" id="' + v.key + '"></div>';
				formHTML = formHTML + '</div>';
			});
			}
			formHTML = formHTML + '</form>';
			div.innerHTML = formHTML;
			console.log(div);
		}
	});
	return true;
}

		</script>

		<script>
function testNullValues(typeOf) {
	var x = '';
	console.log('type Of ' + typeOf);
	x = document.getElementById(typeOf);
	console.log(x.length);
	var text = "";
	sourceFlag = 0;
	var i;
	for(i = 0; i < x.length; i++) {
		console.log('value for element is ' + x.elements[i].value);
		if(x.elements[i].value == '' || x.elements[i].value == null) {
			sourceFlag = 1;
		}
	}
}



		</script>

		<script>
function jtableIntoMap(typeProp, typeDiv) {
	var div = '';
	div = document.getElementById(typeDiv);
	$('div .jtable-data-row').each(function() {
		console.log(this);
		$(this).addClass('jtable-row-selected');
		$(this).addClass('ui-state-highlight');
	});

	var $selectedRows = $(div).jtable('selectedRows');
	$selectedRows.each(function() {
		var record = $(this).data('record');
		var keys = typeProp + record.columnName;
		console.log(keys);
		map[keys] = record.dataType;
		console.log(map);
	});
	$('.jtable-row-selected').removeClass('jtable-row-selected');
}

		</script>
				<script>
        function jtableIntoMapForBase(typeDiv) {
        	var div = '';
        	div = document.getElementById(typeDiv);
        	$('div .jtable-data-row').each(function() {
        		console.log(this);
        		$(this).addClass('jtable-row-selected');
        		$(this).addClass('ui-state-highlight');
        	});

        	var $selectedRows = $(div).jtable('selectedRows');
        	$selectedRows.each(function() {
        		var record = $(this).data('record');
        		console.log(record.columnName);
        		map["transform_"+record.columnName] = record.transformations;
        		map["stagedatatype_"+record.columnName] = record.dataType;
        		map["baseaction_"+record.columnName] = record.dataType;
        		map["partition_"+record.columnName] = record.partition;
        		console.log(map);
        	});
        	$('.jtable-row-selected').removeClass('jtable-row-selected');
        }

        		</script>

		<script>
function formIntoMap(typeProp, typeOf) {
	var x = '';
	x = document.getElementById(typeOf);
	console.log(x);
	var text = "";
	var i;
	for(i = 0; i < x.length; i++) {
		map[typeProp + x.elements[i].name] = x.elements[i].value;
	}
}

		</script>

        <script>
        $(document).ready(function(){
            $( "#enqueueId" ).click(function() {
              console.log("enqueid is clicked");
               $("#filePath").prop("disabled", true);
                $("#enqueueId").prop("disabled", false);
                $('#filePath').val("null");
            });

            $( "#enqueueId1" ).click(function() {
                  console.log("enqueid1 is clicked");
                    $("#enqueueId").prop("disabled", false);

                });

            $( "#filePath" ).click(function() {
                console.log("filePath is clicked");
               $("#enqueueId").prop("disabled", true);
               $("#filePath").prop("disabled", false);
               $('#enqueueId').val("null");
            });

            $( "#filePath1" ).click(function() {
                console.log("filePath is clicked");
               $("#filePath").prop("disabled", false);
            });
          });
           </script>


	</body>

</html>
