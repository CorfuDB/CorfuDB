/// <reference path="../typings/tsd.d.ts"/>

import $ = require('jquery');
jQuery = $;
import bootstrap = require('bootstrap');
import Promise = require('bluebird');
var xhr = require('xhr');

interface CorfuDBView {
    epoch: number;
    logid: string;
    pagesize: number;
    sequencer: string[];
    configmaster: string[];
    segments: CorfuDBSegment[];
    streams: any;
}

interface CorfuDBSegment {
    start: number;
    sealed: number;
    groups: any;
}

var requestID : number = 0;
var view : CorfuDBView;

$(document).ready(function() {
    initialize();
});

function rpc(method: string, params: any) : Promise<any> {
    return new Promise(function (fulfill, reject){
    xhr({
            method: "POST",
            json: {
                method: method,
                params: params,
                jsonrpc: "2.0",
                id: requestID++
            },
            uri: '/control',
            headers: {
                'Content-Type' : 'application/json'
                }
            }, function (err, resp, body)
            {
                if (err != null)
                {
                    reject(err);
                }
                fulfill(body);
            }
        );
    });
}

function retrieveView() : Promise<CorfuDBView> {
    return new Promise<CorfuDBView>(function (fulfill : any, reject){
    xhr({
            method: "GET",
            uri: '/corfu'
            }, function (err, resp, body)
            {
                if (err != null)
                {
                    reject(err);
                }
                var result : CorfuDBView = <CorfuDBView>JSON.parse(body);
                fulfill(result);
            }
        );
    });

}

function updateView(v : CorfuDBView) {
    console.log("logid id " + v.logid);
    $("#logid").text(v.logid);
    $("#epoch").text(v.epoch);
    $("#sequencers").empty();
    v.sequencer.forEach(function(itm, idx, array)
    {
        console.log(itm);
        $("#sequencers").append("<li class='list-group-item'>" + itm + '</li>');
    })
    }

enum Panels {
    MAIN,
    LOGINFO,
    REMOTELOG,
    STREAMINFO
};

function switchPanel(newPanel: Panels)
{
    $(".content").addClass('hidden');
    $("#" + Panels[newPanel]).removeClass('hidden');
}

function updateStreams() : Promise<any>
{
    return rpc("streaminfo", {}).then(function (data) {
        $("#streamtable").empty();
        data.result.forEach(function(itm,idx,array) {
        $("#streamtable").append(
        `<tr><td>${itm.streamid}</td>
            <td>${itm.currentlog}</td>
            <td>${itm.startlog}</td>
            <td>${itm.startpos}</td>
            <td>${itm.epoch}</tr>`);
        });
    })
}

function registerButtonHandlers() {
$("#reset").on('click', function() {
    rpc("reset", {}).then(function(data) {
    location.reload();
    });
});

function updateLogDetail(pos: number) : Promise<any>
{
    return new Promise(function (fulfill,reject) {
    rpc("loginfo", { pos : pos }).then(function(data) {
        $("#payload").addClass("hidden");
        $("#loginfotable").empty();
        $("#loginfotable").append("<tr><td>state</td><td>" + data.result.state + "</a></td></tr>");
        if (data.result.classname != null)
        {
            $("#loginfotable").append("<tr><td>class</td><td>" + data.result.classname + "</a></td></tr>");
        }
        if( data.result.data !== undefined)
        {
            var datakeys = Object.keys(data.result.data);
            datakeys.forEach(function (itm, idx, array) {
                $("#loginfotable").append("<tr><td>" + itm+ "</td><td>" + data.result.data[itm] + "</a></td></tr>");
            });
        }
        if (data.result.error !== undefined)
        {
              $("#loginfotable").append("<tr><td>Error</td><td>" + data.result.error + "</a></td></tr>");
        }
        if (data.result.payload !== undefined)
        {
            $("#payload").removeClass("hidden");
            $("#payloadinfotable").empty();
            var payloadkeys = Object.keys(data.result.payload);
            payloadkeys.forEach(function (itm, idx, array) {
            $("#payloadinfotable").append(`<tr><td>${itm}</td><td>${data.result.payload[itm]}</td></tr>`);
            })
        }
        fulfill(null);
        });
    });
}

$("#remotes").on('click', function() {
    $("#remotelogtable").empty();
    rpc("getalllogs", {}).then(function(data) {
        var keys = Object.keys(data.result);
        keys.forEach(function (itm, idx, array) {
        var addr = data.result[itm].replace("cdbcm", "http");
        $("#remotelogtable").append("<tr><td>" + itm + "</td><td><a href='"+ addr +"'>" + data.result[itm] + "</a></td></tr>");
        });
        switchPanel(Panels.REMOTELOG);
    });
});
$("#overview").on('click', function() {
    switchPanel(Panels.MAIN);
})
$("#log").on('click', function() {
    updateLogDetail(0).then(function() {
        switchPanel(Panels.LOGINFO);
    });
});
$("#streams").on('click', function() {
    updateStreams().then(function() {
        switchPanel(Panels.STREAMINFO);
    });
});
$("#loginfopage").on('click', function(e) {
    e.preventDefault();
    updateLogDetail(parseInt($("#loginfopos").val()));
});
}

function initialize() : Promise<any> {
    rpc('ping', {}).then(function (res)
    {
        console.log(res);
     });
    retrieveView().then(function(v)
    {
        view = v;
        updateView(v);
     });
    registerButtonHandlers();
    return new Promise(function (fulfill,reject)
    {
        fulfill(null);
    })
}
