/* Script for Index.htm 
 * 
 * FUNCTIONS
 * drawChart: generate Google Chart for specified graph type
 * getGraph: request graph data from sevrer based on user input
 * colorField: turn input fields on/off depend on graph type
 * timeFill: fill in value of endDate field using value of startDate
 * createRouteList: request data for route list drop-down menu
 */


google.load("visualization", "1", {packages:["corechart"]});
google.load('visualization', '1', { 'packages': ['map'] });
google.setOnLoadCallback(getGraph);


function drawChart(chart_data, graphType, routeName, startInt, endInt) {
    var chart1_data = new google.visualization.DataTable(chart_data);

    var chart1_options;
    switch (graphType) {
        case 'busfreq':
            chart1_options = {
                title: 'Bike Frequency vs. Time Period for BIXI-Stations ' + routeName,
                width: '100%',
                height: 500,
                //format: 'hh:mm',
                hAxis: {
                    title: 'Time',
                    //format: 'h:mm a',
                    direction:1, 
                    slantedText:true, 
                    slantedTextAngle:20,
                    textStyle:  {fontName: 'Arial', fontSize: 12, bold: false}
                },
                vAxis: {
                    title: 'Frequency (at 1-hr interval)',
                    format:'#'
                },
                animation: {startup: true, duration:500, out:'linear'},
                legend: {position: 'none'},
            };
            break;
        case 'freqsum':
            chart1_options = {
                title: 'Top 10 Highest Cumulative Bikes Frequency per Route From [' + 
                        startInt + '] to [' + endInt + ']',
                width: '100%',
                height: 500,
                //format: 'hh:mm',
                hAxis: {
                    title: 'BIXI-Stations',
                    //format: 'h:mm a',
                    direction: -1, 
                    slantedText: true, 
                    slantedTextAngle: 20,
                    textStyle:  {fontName: 'Arial', fontSize: 12, bold: false}
                },
                vAxis: {
                    title: 'Frequency'
                },
                animation: {startup: true, duration:500, out:'linear'},
                legend: {position: 'none'}
            };
            break;
        case 'trvtime':
            chart1_options = {
                title: 'Travel Time vs. Time Period for Route ' + routeName,
                width: '100%',
                height: 500,
                //format: 'hh:mm',
                hAxis: {
                    title: 'Time',
                    //format: 'h:mm a',
                    direction:1,
                    slantedText:true,
                    slantedTextAngle:20,
                    textStyle:  {fontName: 'Arial', fontSize: 12, bold: false}
                },
                vAxis: {
                    title: 'Travel Time (minutes)',
                    format: '#',
                    viewWindow: {min: 0}
                },
                animation: {startup: true, duration:500, out:'linear'},
                legend: {position: 'none'},
            };
            break;
        case 'trvsum':
            chart1_options = {
                title: 'Top 10 Highest Average Travel Times per Route From [' +
                        startInt + '] to [' + endInt + ']',
                width: '100%',
                height: 500,
                //format: 'hh:mm',
                hAxis: {
                    title: 'BIXI-Stations',
                    //format: 'h:mm a',
                    direction: -1,
                    slantedText: true,
                    slantedTextAngle: 20,
                    textStyle:  {fontName: 'Arial', fontSize: 12, bold: false}
                },
                vAxis: {
                    title: 'Travel Time (minutes)'
                },
                animation: {startup: true, duration:500, out:'linear'},
                legend: {position: 'none'}
            };
            break;
        case 'busdens':
            chart1_options = {
                title: 'Top 10 Locations with Highest Bikes Density From [' + 
                        startInt + '] to [' + endInt + ']',
                width: '100%',
                height: 500,
                mapType: 'styledMap',
                showTip: true,
                useMapTypeControl: true,
                maps: { // custom mapTypeId
                    styledMap: {
                        name: 'Styled Map', // This name will be displayed in the map type control.
                        styles: [
                            {featureType: 'all',
                            stylers: [{visibility:'simplified'}]
                            },
                            {featureType: 'landscape',
                            stylers: [{hue: '#fff'}, {saturation: 0}, {lightness: 40}]
                            },
                            {featureType: 'poi',
                            stylers: [{visibility: 'off'}]
                            },
                            {featureType: 'road',
                            elementType: 'labels.icon',
                            stylers: [{visibility: 'off'}]
                            }
                        ]}},
                icons: {
                    default: {
                        normal: 'http://goo.gl/2pSk0V?gdriveurl',
                        selected: 'http://goo.gl/P4KUPP?gdriveurl' //'http://goo.gl/119yPH?gdriveurl'
                    }
                }
            };
            break;
        case 'busline':
            chart1_options = {
                showLine: true,
                lineWidth: 10,
                lineColor: 'red',
                width: '100%',
                height: 500,
                mapType: 'normal',
                showTip: true,
                useMapTypeControl: true,
                icons: {
                    default: {
                        normal: 'http://goo.gl/VYzz6i?gdriveurl',
                        selected: 'http://goo.gl/uE6vJI?gdriveurl'
                    }
                }
            };
            break;
        default:
            chart1_options = {
                title: chart1_main_title,
                vAxis: {title: chart1_vaxis_title,  titleTextStyle: {color: 'red'}}
            };
    };

    var chart1_chart;
    if (graphType == 'busdens' || graphType == 'busline') {
        //window.alert("goint to draw")
        chart1_chart = new google.visualization.Map(document.getElementById('ttc_chart'));
        //chart1_chart.draw(chart1_data, chart1_options);
        document.getElementById('BIXI_chart').innerHTML = '';
        //window.alert('drew')
    } else {
        chart1_chart = new google.visualization.ColumnChart(document.getElementById('ttc_chart'));
        document.getElementById('BIXI_chart').innerHTML = '';
    }
    chart1_chart.draw(chart1_data, chart1_options);

    return false;
}

function getGraph() {
    $("#inputForm").submit(function(e) {
        e.preventDefault();
    });

    $('#button').prop('value', 'Loading');
    
    var startDate = document.getElementById("startDate").value;
    var startTime = document.getElementById("startTime").value;
    var endDate = document.getElementById("endDate").value;
    var endTime = document.getElementById("endTime").value;
    var routeTag = document.getElementById("routeTag").value;
    var graphType = document.getElementById("graphType").value;

    var routeName;
    $.ajax({
        type: "POST",
        //dataType: "json",
        url: $SCRIPT_ROOT + '/ttcname/' + routeTag,
        async: false,
        success: function(data){
            //window.alert('data');
            if(data){
                routeName = data;
            }
        },
    });

    var timeFormat = "HH:MM:SS"
    if (startTime.length < timeFormat.length) {
        startTime += ":00"
        endTime += ":00"
    }

    var urlStr = $SCRIPT_ROOT + '/bixidata/' + graphType + '/' + startDate + ' ' + startTime + '/' +
                endDate + ' ' + endTime + '/' + routeTag;

    var chart_data;
    $.ajax({
        type: "POST",
        dataType: "json",
        url: urlStr,
        async: false,
        success: function(data){
            //window.alert('data');
            if(data){
                chart_data = data;
                //chart_data = $.parseJSON(data);
                //window.alert('got data');
                drawChart(chart_data, graphType, routeName, startDate + ' ' + startTime, endDate + ' ' + endTime);
            }
        },
    });


    $('#button').prop('value', 'Submit');
    //end
    return false;
}


function colorField(){
    var graphType = document.getElementById("graphType").value;

    if (graphType === 'busfreq' || graphType === 'trvtime') {
        document.getElementById('station_id').style.display = 'block';
        document.getElementById('routeTagLb').style.display = 'block';
        document.getElementById('note_1').style.display = 'none';
    } else if (graphType === 'trvsum') {
        document.getElementById('note_1').style.display = 'block';
    } else if (graphType === 'busline') {
        //document.getElementById('busid').style.display = 'block';
        document.getElementById('station_id').style.display = 'block';
        document.getElementById('routeTagLb').style.display = 'block';
        document.getElementById('note_1').style.display = 'none';

        //$('#startTime').attr('value', '10:00:00');
    } else {
        document.getElementById('routeTag').style.display = 'none';
        document.getElementById('routeTagLb').style.display = 'none';
        //document.getElementById('busid').style.display = 'none';
        document.getElementById('note_1').style.display = 'none';
    }
}


function timeFill() {
    var startDate = document.getElementById("startDate").value;
    document.getElementById("endDate").value = startDate;
}


window.onload = function createRouteList() {
    $.ajax({
        type: "POST",
        dataType: "json",
        url: $SCRIPT_ROOT + '/ttclist',
        async: false,
        success: function(data){
            //window.alert('data');
            if(data){
                for (var field in data) {
                    //alert(data[field].routeTag + '\t' + data[field].routeName);
                    $('<option value="'+ data[field].routeTag +'">' + data[field].routeTag + ' - ' + data[field].routeName + '</option>').appendTo('#station_id');
                }
            }
        },
    });
}

