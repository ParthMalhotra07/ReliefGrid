#!/usr/bin/env python3
"""Build the review draft with native vector figures and readable table text.

Requires reportlab==5.0.1. It does not run workloads, resolve pending evidence,
or declare final submission acceptance. Input is docs/report/submission-draft.md.
"""
import argparse
from html import escape
from pathlib import Path
import re

from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.graphics import renderSVG
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import BaseDocTemplate, Frame, KeepTogether, PageBreak, PageTemplate, Paragraph, Spacer, Table, TableStyle

INK=colors.HexColor('#173249'); BLUE=colors.HexColor('#e8f1fa'); GREY=colors.HexColor('#f3f5f7'); EDGE=colors.HexColor('#557086')
WIDTH=A4[0]-84

def label(d,x,y,text,size=8.5,bold=False,center=True):
    lines=text.split('\n')
    for i,line in enumerate(lines):
        d.add(String(x,y+(len(lines)-1)*size*.58-i*size*1.16,line,fontName='Helvetica-Bold' if bold else 'Helvetica',fontSize=size,fillColor=INK,textAnchor='middle' if center else 'start'))

def box(d,x,y,w,h,text,shade=BLUE,size=8.5):
    d.add(Rect(x,y,w,h,fillColor=shade,strokeColor=EDGE,strokeWidth=.65))
    label(d,x+w/2,y+h/2-2,text,size)

def arrow(d,x1,y1,x2,y2,dashed=False):
    d.add(Line(x1,y1,x2,y2,strokeColor=EDGE,strokeWidth=.7,strokeDashArray=[3,2] if dashed else None))
    import math
    a=math.atan2(y2-y1,x2-x1);s=4
    d.add(Polygon([x2,y2,x2-s*math.cos(a-.45),y2-s*math.sin(a-.45),x2-s*math.cos(a+.45),y2-s*math.sin(a+.45)],fillColor=EDGE,strokeColor=EDGE))

def line(d,x1,y1,x2,y2):d.add(Line(x1,y1,x2,y2,strokeColor=EDGE,strokeWidth=.65))

def figure1():
    d=Drawing(WIDTH,245)
    box(d,8,190,75,40,'Browser',GREY)
    box(d,108,190,85,40,'API',GREY)
    box(d,228,190,110,40,'Coordinator\nring + leader discovery',GREY)
    arrow(d,83,210,108,210);arrow(d,193,210,228,210)
    label(d,95,219,'HTTP',7);label(d,211,219,'HTTP',7)
    d.add(Rect(3,80,WIDTH-6,93,fillColor=None,strokeColor=EDGE,strokeWidth=.5))
    label(d,WIDTH/2,160,'Per-shard consensus authority',9,True)
    for x,n in ((12,0),(183,1),(354,2)):
        box(d,x,92,145,55,f'Shard {n}: 3 voters\nRaft -> SQLite FSM\ncommitted outbox')
        line(d,283,190,283,181);line(d,84.5,181,426.5,181);arrow(d,x+72.5,181,x+72.5,147)
        line(d,x+72.5,92,x+72.5,70);line(d,x+72.5,70,195,70)
    box(d,145,12,100,38,'Outbox relay',GREY);arrow(d,195,70,195,50)
    box(d,280,12,95,38,'RabbitMQ',GREY);arrow(d,245,31,280,31)
    box(d,405,12,100,38,'Matcher /\nreconcilers',GREY);arrow(d,375,31,405,31)
    label(d,261,43,'publish',7);label(d,390,43,'deliver',7)
    line(d,505,31,WIDTH-1,31);line(d,WIDTH-1,31,WIDTH-1,241);line(d,WIDTH-1,241,283,241);arrow(d,283,241,283,230)
    label(d,400,233,'claims / transitions',7.5)
    return d

def figure2():
    d=Drawing(WIDTH,270)
    d.add(Rect(2,140,WIDTH-4,117,fillColor=None,strokeColor=EDGE,strokeWidth=.5))
    label(d,WIDTH/2,245,'One committed apply transaction; five physical tables',9,True)
    for x,text in ((8,'records\nkey / kind / version\nJSON / tombstone'),(181,'operation_results\noperationId / hash\nhistorical result'),(354,'attempt_closures\nresource + attempt\npermanent fence')):
        box(d,x,186,149,45,text,GREY,8.2)
    box(d,90,146,149,32,'outbox: publication work',GREY,8.2);box(d,271,146,149,32,'meta: schema + command index',GREY,8.0)
    box(d,4,85,65,35,'WAITING',size=8.2);box(d,88,85,75,35,'MATCHING',size=8.2)
    box(d,189,85,70,35,'MATCHED',size=8.2);box(d,286,85,85,35,'IN_PROGRESS',size=8)
    box(d,395,85,WIDTH-399,35,'CANCELLING /\nFULFILLING',size=8)
    for start,end in ((69,88),(163,189),(259,286),(371,395)):arrow(d,start,102.5,end,102.5)
    line(d,224,120,224,130);line(d,224,130,447,130);arrow(d,447,130,447,120)
    box(d,100,20,80,35,'RETRYING',size=8.2);arrow(d,125.5,85,140,55)
    line(d,100,37.5,36.5,37.5);arrow(d,36.5,37.5,36.5,85)
    label(d,95,7,'committed closure before retarget',7)
    box(d,395,20,WIDTH-399,35,'CANCELLED /\nFULFILLED',size=8)
    arrow(d,447,85,447,55)
    box(d,220,20,150,35,'Attempt-bound resource claim',size=8.1)
    arrow(d,224,85,260,55,True);arrow(d,328,85,325,55,True)
    label(d,295,7,'reciprocal while assigned',7)
    arrow(d,45,146,36.5,120,True);label(d,49,135,'JSON state',7)
    return d

def figure3():
    d=Drawing(WIDTH,215)
    label(d,WIDTH/2,204,'Publication',9,True)
    for x,text in ((5,'State + outbox\ncommit'),(135,'Mandatory\npublish'),(265,'Routed broker\nconfirmation'),(395,'Published\nmark')):box(d,x,155,110,36,text,GREY)
    for a,b in ((115,135),(245,265),(375,395)):arrow(d,a,173,b,173)
    label(d,WIDTH/2,136,'Work delivery / matching',9,True)
    for x,w,text in ((5,85,'Durable intent'),(110,85,'TryClaim'),(215,85,'Reciprocal\nconfirmation'),(320,85,'Durable\nhandoff'),(425,80,'Delivery ack')):box(d,x,87,w,35,text)
    for a,b in ((90,110),(195,215),(300,320),(405,425)):arrow(d,a,104.5,b,104.5)
    box(d,5,15,100,37,'Unknown\noutcome',GREY)
    box(d,130,15,115,37,'Same operationId\nlookup / retry',GREY)
    box(d,280,15,105,37,'Committed\nCloseAttempt',GREY)
    box(d,405,15,100,37,'New attempt /\nintent',GREY)
    arrow(d,152.5,87,55,52);arrow(d,105,33.5,130,33.5)
    arrow(d,245,33.5,280,33.5,True);arrow(d,385,33.5,405,33.5)
    label(d,270,64,'retry only the unresolved stage; close before retarget',7.5)
    return d

def figure4():
    d=Drawing(WIDTH,255)
    label(d,WIDTH/2,245,'One physical host / five kind nodes',9,True)
    box(d,205,203,100,30,'Control plane',GREY)
    for x,n in ((5,1),(125,2),(245,3)):
        box(d,x,115,110,70,f'Data worker {n}\nS0 voter / S1 voter\nS2 voter',size=8.2)
    box(d,370,115,135,70,'Infra worker\nCoordinator / RabbitMQ\nTraefik / observability',GREY,8.2)
    line(d,255,203,255,194);line(d,60,194,437.5,194)
    for x in (60,180,300,437.5):arrow(d,x,194,x,185)
    label(d,WIDTH/2,101,'API/matcher Deployments: chart-controlled placement',8)
    box(d,5,37,145,50,'Per-voter PVC\nRaft log / stable store\ncompleted snapshots',GREY,8.2)
    box(d,180,37,145,50,'emptyDir SQLite\nrebuilt on startup',GREY,8.2)
    arrow(d,150,62,180,62)
    box(d,350,37,155,50,'Empty disk: unadmitted\nguarded replacement\ncatch up, then promote',size=8.2)
    label(d,WIDTH/2,17,'Separate Raft, data HTTP, operator and restricted metrics interfaces',8)
    return d

STYLES={
    'title':ParagraphStyle('title',fontName='Helvetica-Bold',fontSize=18,leading=21,textColor=INK,spaceAfter=8),
    'h2':ParagraphStyle('h2',fontName='Helvetica-Bold',fontSize=13.5,leading=16,textColor=INK,spaceBefore=7,spaceAfter=7,keepWithNext=True),
    'body':ParagraphStyle('body',fontName='Times-Roman',fontSize=11.1,leading=14,spaceAfter=6),
    'tablecaption':ParagraphStyle('tablecaption',fontName='Times-Italic',fontSize=9.5,leading=11.5,spaceAfter=6,keepWithNext=True),
    'caption':ParagraphStyle('caption',fontName='Times-Italic',fontSize=9,leading=11,spaceAfter=7),
    'table':ParagraphStyle('table',fontName='Times-Roman',fontSize=9,leading=11),
    'th':ParagraphStyle('th',fontName='Helvetica-Bold',fontSize=9,leading=11,textColor=INK),
    'ref':ParagraphStyle('ref',fontName='Times-Roman',fontSize=9.1,leading=11.2,spaceAfter=4),
    'meta':ParagraphStyle('meta',fontName='Helvetica',fontSize=9,leading=11.5,spaceAfter=6,textColor=INK),
}

def markup(text):
    out=[];last=0
    for match in re.finditer(r'\[([^\]]+)\]\((https://[^\s)]+)\)',text):
        out.append(escape(text[last:match.start()]))
        out.append('<link href="'+escape(match[2],quote=True)+'" color="#224f77">'+escape(match[1])+'</link>')
        last=match.end()
    out.append(escape(text[last:]))
    return ''.join(out)

def table(rows):
    count=len(rows[0]);widths=([110,245,WIDTH-355] if count==3 else [110,135,165,WIDTH-410])
    if count==4 and rows[0][0]=='Decision':widths=[100,105,220,WIDTH-425]
    if count==3 and 'PS internal' in rows[0][0]:widths=[125,230,WIDTH-355]
    if count==3 and rows[0][0].startswith('Demo'):widths=[135,WIDTH-225,90]
    if count==3 and rows[0][0]=='Fault':widths=[135,265,WIDTH-400]
    cells=[[Paragraph(markup(cell),STYLES['th' if i==0 else 'table']) for cell in row] for i,row in enumerate(rows)]
    t=Table(cells,colWidths=widths,repeatRows=1,hAlign='LEFT')
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),BLUE),('VALIGN',(0,0),(-1,-1),'TOP'),('LINEBELOW',(0,0),(-1,0),.6,EDGE),('LINEBELOW',(0,1),(-1,-1),.25,colors.HexColor('#cdd6dd')),('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    return t

def footer(canvas,doc):
    canvas.saveState();canvas.setFont('Helvetica',8);canvas.setFillColor(EDGE)
    canvas.drawString(42,24,'ReliefGrid - review draft; final evidence/source pin pending')
    canvas.drawRightString(A4[0]-42,24,str(doc.page))
    canvas.restoreState()

def build(root,output):
    source=root/'docs/report/submission-draft.md';text=source.read_text(encoding='utf-8')
    if '{FINAL_SHA}' in text or 'TODO-after' in text:raise ValueError('Unresolved submission citation')
    figures={f'F{i}':fn() for i,fn in enumerate((figure1,figure2,figure3,figure4),1)}
    for key,figure in figures.items():renderSVG.drawToFile(figure,str(root/f'docs/report/figures/{key}-print.svg'))
    story=[]
    for page_no,page in enumerate(text.split('<!-- page -->')):
        if page_no:story.append(PageBreak())
        lines=page.strip().splitlines();i=0
        while i<len(lines):
            line_text=lines[i].strip()
            if not line_text:i+=1;continue
            if line_text.startswith('|'):
                rows=[]
                while i<len(lines) and lines[i].strip().startswith('|'):
                    row=[cell.strip() for cell in lines[i].strip().strip('|').split('|')]
                    if not all(re.fullmatch(r':?-+:?',cell) for cell in row):rows.append(row)
                    i+=1
                story.extend([table(rows),Spacer(1,7)]);continue
            if line_text.startswith('!['):
                key=re.search(r'!\[(F\d)',line_text)[1]
                i+=1
                while i<len(lines) and not lines[i].strip():i+=1
                group=[figures[key],Spacer(1,5)]
                if i<len(lines) and re.match(r'F\d\.',lines[i].strip()):
                    group.append(Paragraph(markup(lines[i].strip()),STYLES['caption']))
                    i+=1
                story.append(KeepTogether(group));continue
            if line_text.startswith('# '):story.append(Paragraph(markup(line_text[2:]),STYLES['title']));i+=1;continue
            if line_text.startswith('## '):story.append(Paragraph(markup(line_text[3:]),STYLES['h2']));i+=1;continue
            paragraph=[line_text];i+=1
            while i<len(lines) and lines[i].strip() and not lines[i].lstrip().startswith(('#','|','![')):
                paragraph.append(lines[i].strip());i+=1
            content=' '.join(paragraph)
            style='tablecaption' if re.match(r'T\d',content) else 'caption' if re.match(r'F\d\.',content) else 'ref' if re.match(r'\[\d+\]',content) else 'meta' if content.startswith(('Working submission','Parth Malhotra')) else 'body'
            story.append(Paragraph(markup(content),STYLES[style]))
    output.parent.mkdir(parents=True,exist_ok=True)
    doc=BaseDocTemplate(str(output),pagesize=A4,leftMargin=42,rightMargin=42,topMargin=36,bottomMargin=42,title='ReliefGrid submission review draft',author='Parth Malhotra and Hrishabh Sharrma',pageCompression=1)
    doc.addPageTemplates(PageTemplate(id='report',frames=[Frame(42,42,WIDTH,A4[1]-78,leftPadding=0,rightPadding=0,topPadding=0,bottomPadding=0)],onPage=footer))
    doc.build(story)
    print('Built',output)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);parser.add_argument('--output',type=Path)
    args=parser.parse_args();build(args.root,args.output or args.root/'docs/report/reliefgrid-submission-draft.pdf')
