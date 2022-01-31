# Run VBA code in Outlook to bulk save attachments from multiple emails

# Save Exact Same Name Attachments Directly

Dim GCount As Integer
Dim GFilepath As String
Public Sub SaveAttachments()
'Update 20200821
Dim xMailItem As Outlook.MailItem
Dim xAttachments As Outlook.Attachments
Dim xSelection As Outlook.Selection
Dim i As Long
Dim xAttCount As Long
Dim xFilePath As String, xFolderPath As String, xSaveFiles As String
On Error Resume Next
xFolderPath = CreateObject("WScript.Shell").SpecialFolders(16)
Set xSelection = Outlook.Application.ActiveExplorer.Selection
xFolderPath = xFolderPath & "\Attachments\"
If VBA.Dir(xFolderPath, vbDirectory) = vbNullString Then
    VBA.MkDir xFolderPath
End If
GFilepath = ""
For Each xMailItem In xSelection
    Set xAttachments = xMailItem.Attachments
    xAttCount = xAttachments.Count
    xSaveFiles = ""
    If xAttCount > 0 Then
        For i = xAttCount To 1 Step -1
            GCount = 0
            xFilePath = xFolderPath & xAttachments.Item(i).FileName
            GFilepath = xFilePath
            xFilePath = FileRename(xFilePath)
            If IsEmbeddedAttachment(xAttachments.Item(i)) = False Then
                xAttachments.Item(i).SaveAsFile xFilePath
                If xMailItem.BodyFormat <> olFormatHTML Then
                    xSaveFiles = xSaveFiles & vbCrLf & "<Error! Hyperlink reference not valid.>"
                Else
                    xSaveFiles = xSaveFiles & "<br>" & "<a href='file://" & xFilePath & "'>" & xFilePath & "</a>"
                End If
            End If
        Next i
        If xSaveFiles <> "" Then
            If xMailItem.BodyFormat <> olFormatHTML Then
                xMailItem.Body = vbCrLf & "The file(s) were saved to " & xSaveFiles & vbCrLf & xMailItem.Body
            Else
                xMailItem.HTMLBody = "<p>" & "The file(s) were saved to " & xSaveFiles & "</p>" & xMailItem.HTMLBody
            End If
        End If
        xMailItem.Save
    End If
Next
Set xAttachments = Nothing
Set xMailItem = Nothing
Set xSelection = Nothing
End Sub
 
Function FileRename(FilePath As String) As String
Dim xPath As String
Dim xFso As FileSystemObject
On Error Resume Next
Set xFso = CreateObject("Scripting.FileSystemObject")
xPath = FilePath
FileRename = xPath
If xFso.FileExists(xPath) Then
    GCount = GCount + 1
    xPath = xFso.GetParentFolderName(GFilepath) & "\" & xFso.GetBaseName(GFilepath) & " " & GCount & "." + xFso.GetExtensionName(GFilepath)
    FileRename = FileRename(xPath)
End If
xFso = Nothing
End Function
 
Function IsEmbeddedAttachment(Attach As Attachment)
Dim xItem As MailItem
Dim xCid As String
Dim xID As String
Dim xHtml As String
On Error Resume Next
IsEmbeddedAttachment = False
Set xItem = Attach.Parent
If xItem.BodyFormat <> olFormatHTML Then Exit Function
xCid = ""
xCid = Attach.PropertyAccessor.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F")
If xCid <> "" Then
    xHtml = xItem.HTMLBody
    xID = "cid:" & xCid
    If InStr(xHtml, xID) > 0 Then
        IsEmbeddedAttachment = True
    End If
End If
End Function

# Check For Duplicates
Public Sub SaveAttachments()
'Update 20200821
Dim xMailItem As Outlook.MailItem
Dim xAttachments As Outlook.Attachments
Dim xSelection As Outlook.Selection
Dim i As Long
Dim xAttCount As Long
Dim xFilePath As String, xFolderPath As String, xSaveFiles As String
Dim xYesNo As Integer
Dim xFlag As Boolean
On Error Resume Next
xFolderPath = CreateObject("WScript.Shell").SpecialFolders(16)
Set xSelection = Outlook.Application.ActiveExplorer.Selection
xFolderPath = xFolderPath & "\Attachments\"
If VBA.Dir(xFolderPath, vbDirectory) = vbNullString Then
    VBA.MkDir xFolderPath
End If
For Each xMailItem In xSelection
    Set xAttachments = xMailItem.Attachments
    xAttCount = xAttachments.Count
    xSaveFiles = ""
    If xAttCount > 0 Then
        For i = xAttCount To 1 Step -1
            xFilePath = xFolderPath & xAttachments.Item(i).FileName
            xFlag = True
            If VBA.Dir(xFilePath, 16) <> Empty Then
                xYesNo = MsgBox("The file is exists, do you want to replace it", vbYesNo + vbInformation, "Kutools for Outlook")
                If xYesNo = vbNo Then xFlag = False
            End If
            If xFlag = True Then
                xAttachments.Item(i).SaveAsFile xFilePath
                If xMailItem.BodyFormat <> olFormatHTML Then
                    xSaveFiles = xSaveFiles & vbCrLf & "<Error! Hyperlink reference not valid.>"
                Else
                    xSaveFiles = xSaveFiles & "<br>" & "<a href='file://" & xFilePath & "'>" & xFilePath & "</a>"
                End If
            End If
        Next i
        If xSaveFiles <> "" Then
            If xMailItem.BodyFormat <> olFormatHTML Then
                xMailItem.Body = vbCrLf & "The file(s) were saved to " & xSaveFiles & vbCrLf & xMailItem.Body
            Else
                xMailItem.HTMLBody = "<p>" & "The file(s) were saved to " & xSaveFiles & "</p>" & xMailItem.HTMLBody
            End If
        End If
        xMailItem.Save
    End If
Next
Set xAttachments = Nothing
Set xMailItem = Nothing
Set xSelection = Nothing
End Sub
