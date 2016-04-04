Function Main()
    port = CreateObject("roMessagePort")
    posterScreen = CreateObject("roPosterScreen")
    posterScreen.SetTitle("CarbonDVR")
    posterScreen.SetBreadcrumbEnabled(false)
    posterScreen.SetBreadcrumbText("[location1]", "[location2]")
'   arced-landscape is cool looking, but seems to have occassional pauses when scrolling, especially on series 1 hardware
'   flat-category seems to be faster
'    posterScreen.SetListStyle("arced-landscape")
    posterScreen.SetListStyle("flat-category")
    posterScreen.SetListDisplayMode("scale-to-fit")
    posterScreen.SetMessagePort(port)

    categoryList = CreateObject("roArray", 2, true)
    categoryList.push("All Shows")
    categoryList.push("Shows With New Episodes")
    posterScreen.SetListNames(categoryList)

    allShowsURL ="http://trinos.trinaria.com:8085/shows"
    newShowsURL ="http://trinos.trinaria.com:8085/shows/new"

    selectedCategory = 0
    focusItemNew = 0
    focusItemAll = 0
 
    bRepopulateList = true
    bFirstLoad = true

    while True

        if bRepopulateList then
            print "populating show lists"
            showListNew = FetchShowList(newShowsURL)
            if focusItemNew >= showListNew.count()
                focusItemNew = 0
            end if
            showListAll = FetchShowList(allShowsURL)
            if focusItemAll >= showListAll.count()
                focusItemAll = 0
            end if
            bRepopulateList = false
            bRedisplayList = true
        end if

        if bRedisplayList then
            print "redisplaying show list"
            if selectedCategory = 1 then
                print "displaying shows with new episodes"
                posterScreen.SetContentList(showListNew)
                if showListNew.count() > 0 then
                    posterScreen.SetFocusedListItem(focusItemNew)
                else
                    posterScreen.ShowMessage("No recordings")
                end if
            else 
                print "displaying all shows"
                posterScreen.SetContentList(showListAll)
                if showListAll.count() > 0 then
                    posterScreen.SetFocusedListItem(focusItemAll)
                else
                    posterScreen.ShowMessage("No recordings")
                end if
            end if
            posterScreen.Show() 
            bRedisplayList = false
        end if

        if bFirstLoad then
            if showListAll.count() > 0 then
                posterScreen.SetFocusToFilterBanner(false)
            end if
            bFirstLoad = false
        end if

        msg = wait(0, port)
        If msg.isScreenClosed() Then
           return -1
        elseif msg.isListFocused()
            selectedCategory = msg.GetIndex()
            bRedisplayList = true
        elseif msg.isListItemFocused()
            if selectedCategory = 1 then
                focusItemNew = msg.GetIndex()
            else
                focusItemAll = msg.GetIndex()
            endif
        elseif msg.isListItemSelected()
            if selectedCategory = 1 then
                showInfo = showListNew[msg.GetIndex()]
            else
                showInfo = showListAll[msg.GetIndex()]
            endif
            if showInfo.springboardURL <> invalid
                print "springboardURL: ";showInfo.springboardURL
                ShowSpringboard(showInfo.springboardURL)
                bRepopulateList = true
            else
                ShowEpisodeList3(showInfo.Title, showInfo.newEpisodeListURL, showInfo.rerunEpisodeListURL, showInfo.archivedEpisodeListURL)
                bRepopulateList = true
            endif
        endif
    end while
End Function
 
 
Function FetchShowList(showListURL as String) AS Object
    posterList = CreateObject("roArray", 10, true)

    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(showListURL)
    showList_xml = urlTransfer.GetToString()
    xml=CreateObject("roXMLElement")
    if not xml.Parse(showList_xml) then
        print "Can't parse showlist xml file"
        return posterList
    endif

    if xml.show = invalid then
        print "no 'show' tag"
        return posterList
    endif

    if GetInterface(xml.show, "ifArray") = invalid
        print "xml file is not formatted correctly"
        return posterList
    endif

    shows = xml.show
    for each show in shows
        poster = CreateObject("roAssociativeArray")
        poster.ContentType = "episode"
        poster.Title = show@title
        poster.Description = show@description
        poster.ShortDescriptionLine1 = show@title
        poster.ShortDescriptionLine2 = show@description
        poster.SDPosterURL = "pkg:/images/genericSDPoster.jpg"
        poster.HDPosterURL = "pkg:/images/genericHDPoster.jpg"
        if show@hd_img <> invalid then
            poster.HDPosterURL = show@hd_img
        endif
	poster.newEpisodeListURL = show@new_episode_list_url
	poster.rerunEpisodeListURL = show@rerun_episode_list_url
	poster.archivedEpisodeListURL = show@archived_episode_list_url
	poster.springboardURL = show@springboard_url
        posterList.Push(poster)
    next

    Return posterList

End Function


Function FetchEpisodePosterList(episodeListURL as String) AS Object
    posterList = CreateObject("roArray", 10, true)

    print "Fetching episode list from "; episodeListURL
    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(episodeListURL)
    episodes_xml = urlTransfer.GetToString()
    xml=CreateObject("roXMLElement")
    if not xml.Parse(episodes_xml) then
        print "Can't parse episodes xml file"
        return posterList
    endif

    if xml.show = invalid then
        print "no 'show' tag"
        return posterList
    endif

    if GetInterface(xml.show, "ifArray") = invalid
        print "xml file is not formatted correctly"
        return posterList
    endif

    shows = xml.show
    for each show in shows
        poster = CreateObject("roAssociativeArray")
        poster.ContentType = "episode"
        poster.ShortDescriptionLine1 = show@short_description_1
        poster.ShortDescriptionLine2 = show@short_description_2
        poster.Description = show@description
        poster.SDPosterURL = "pkg:/images/genericSDPoster.jpg"
        poster.HDPosterURL = "pkg:/images/genericHDPoster.jpg"
        if show@hd_img <> invalid then
            poster.HDPosterURL = show@hd_img
        endif
	poster.springboardURL = show@springboard_url
        posterList.Push(poster)
    next

    Return posterList

End Function


Function GetNumberedFilter(filterName as String, count as Integer) AS String
    if count = 0 then
        return filterName
    endif

    return filterName + " (" + count.tostr() +")"

End Function


Function ShowEpisodeList3(showName as String, newEpisodeListURL as String, rerunEpisodeListURL as String, archivedEpisodeListURL as String)
    port = CreateObject("roMessagePort")
    posterScreen = CreateObject("roPosterScreen")
    posterScreen.SetMessagePort(port)
    posterScreen.SetTitle("CarbonDVR")
    posterScreen.SetBreadcrumbText("", showName)
    posterScreen.SetListStyle("flat-episodic-16x9")


    categoryList = CreateObject("roArray", 2, true)
    categoryList.push("new")
    categoryList.push("rerun")
    categoryList.push("archive")
    posterScreen.SetListNames(categoryList)

    selectedCategory = 0
    focusItemNew = 0
    focusItemRerun = 0
    focusItemArchived = 0

    bRepopulatePosterList = True
    bFirstLoad = true

    while True

        if bRepopulatePosterList then
            print "populating episode list"
            posterListNew = FetchEpisodePosterList(newEpisodeListURL)
            if focusItemNew >= posterListNew.count()
                focusItemNew = 0
            end if
            posterListRerun = FetchEpisodePosterList(rerunEpisodeListURL)
            if focusItemRerun >= posterListRerun.count()
                focusItemRerun = 0
            end if
            posterListArchived = FetchEpisodePosterList(archivedEpisodeListURL)
            if focusItemArchived >= posterListArchived.count()
                focusItemArchived = 0
            end if

            categoryList = CreateObject("roArray", 3, true)
            categoryList.push(GetNumberedFilter("new", posterListNew.count()))
            categoryList.push(GetNumberedFilter("rerun", posterListRerun.count()))
            categoryList.push(GetNumberedFilter("archive", posterListArchived.count()))
            posterScreen.SetListNames(categoryList)

            bRedisplayPosterList = true 
            bRepopulatePosterList = False
        end if

        if bRedisplayPosterList then
            print "redisplaying poster list"
            if selectedCategory = 0 then
                print "displaying new episodes"
                posterScreen.SetContentList(posterListNew)
                if posterListNew.count() > 0 then
                    posterScreen.SetFocusedListItem(focusItemNew)
                else
                    posterScreen.ShowMessage("No recordings")
                end if
            elseif selectedCategory = 1 then
                print "displaying rerun episodes"
                posterScreen.SetContentList(posterListRerun)
                if posterListRerun.count() > 0 then
                    posterScreen.SetFocusedListItem(focusItemRerun)
                else
                    posterScreen.ShowMessage("No recordings")
                end if
            else 
                print "displaying archived episodes"
                posterScreen.SetContentList(posterListArchived)
                if posterListArchived.count() > 0 then
                    posterScreen.SetFocusedListItem(focusItemArchived)
                else
                    posterScreen.ShowMessage("No recordings")
                end if
            end if
            posterScreen.Show() 
            bRedisplayPosterList = false
        end if

        if bFirstLoad then
            if posterListNew.count() > 0 then
                posterScreen.SetContentList(posterListNew)
                posterScreen.SetFocusedList(0)
                posterScreen.SetFocusedListItem(focusItemNew)
                posterScreen.SetFocusToFilterBanner(false)
            elseif posterListRerun.count() > 0 then
                posterScreen.SetContentList(posterListRerun)
                posterScreen.SetFocusedList(1)
                posterScreen.SetFocusedListItem(focusItemRerun)
                posterScreen.SetFocusToFilterBanner(false)
            end if
            bFirstLoad = false
        end if
 
        msg = wait(0, port)
        if msg.isScreenClosed() Then
           return -1
        elseif msg.isListFocused()
            selectedCategory = msg.GetIndex()
            bRedisplayPosterList = true
        elseif msg.isListItemFocused()
            if selectedCategory = 0 then
                focusItemNew = msg.GetIndex()
            elseif selectedCategory = 1 then
                focusItemRerun = msg.GetIndex()
            else
                focusItemArchived = msg.GetIndex()
            endif
        elseif msg.isListItemSelected()
            print "msg.isListItemSelected: index="; msg.GetIndex()
            print "selectedCategory = "; selectedCategory
            if selectedCategory = 0 then
                posterInfo = posterListNew[msg.GetIndex()]
            elseif selectedCategory = 1 then
                posterInfo = posterListRerun[msg.GetIndex()]
            else
                posterInfo = posterListArchived[msg.GetIndex()]
            end if
            if posterInfo.springboardURL.GetString() <> invalid
                ShowSpringboard(posterInfo.springboardURL)
                bRepopulatePosterList = True
            end if
        end if
     end while
End Function




Function FetchSpringboardContent(springboardURL as String)
    springboardContent = CreateObject("roAssociativeArray")

    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(springboardURL)
    springboard_xml = urlTransfer.GetToString()
    xml=CreateObject("roXMLElement")
    if not xml.Parse(springboard_xml) then
        print "Can't parse episodes xml file"
        return springboardContent
    endif

    if xml.show = invalid then
        print "no 'show' tag"
        return springboardContent
    endif
	
'    print "Information: xml.show follows:"
'    print xml.show
	
    show = xml.show[0]
	
'    print "Information: xml.show[0] follows:"
'    print show
	
    springboardContent.ContentType = "episode"
    springboardContent.Title = show@title
    springboardContent.Description = show@description
    springboardContent.SDPosterURL = "pkg:/images/genericSDPoster.jpg"
    springboardContent.HDPosterURL = "pkg:/images/genericHDPoster.jpg"
    if show@hd_img <> invalid then
        springboardContent.HDPosterURL = show@hd_img
    endif
    springboardContent.HDBifUrl = show@hd_bif_url
'    springboardContent.Rating = "NR"
'    springboardContent.StarRating = "75"
    springboardContent.ReleaseDate = show@date_recorded
    springboardContent.Length = show@length
'    springboardContent.Categories = []
'    springboardContent.Categories.Push("[Category1]")
'    springboardContent.Categories.Push("[Category2]")
'    springboardContent.Categories.Push("[Category3]")
    springboardContent.Actors = []
    springboardContent.Actors.Push(show@trintv_episode_number)
'    springboardContent.Director = "[Director]"
    springboardContent.TRINTV_ShowName = show@trintv_showname
    springboardContent.TRINTV_DeleteURL = show@trintv_delete_url
    springboardContent.TRINTV_SetPositionURL = show@trintv_setposition_url
    springboardContent.TRINTV_GetPositionURL = show@trintv_getposition_url
    springboardContent.TRINTV_ArchiveURL = show@trintv_archive_url
    springboardContent.TRINTV_GetArchiveStateURL = show@trintv_getarchivestate_url

    springboardContent.StreamQualities  = CreateObject("roArray", 3, true) 
    springboardContent.StreamBitrates   = CreateObject("roArray", 3, true)
    springboardContent.StreamUrls       = CreateObject("roArray", 3, true)
	
    streams = xml.show.stream
    for each stream in streams
        springboardContent.StreamBitrates.Push(strtoi(stream@bitrate))
        springboardContent.StreamQualities.Push(stream@quality)
        springboardContent.StreamUrls.Push(stream@url)
    next

    return springboardContent
	
End Function


Function EnableSpringboardButtons(springboardScreen as Object, springboardContent as Object)

    springboardScreen.ClearButtons()

    playbackPosition = GetPlaybackPosition(springboardContent.TRINTV_GetPositionURL)
    archiveState = GetArchiveState(springboardContent.TRINTV_GetArchiveStateURL)

    if playbackPosition > 0 then
        springboardScreen.addbutton(1,"Resume playing")
        springboardScreen.addbutton(2,"Play from beginning")
    else
        springboardScreen.addbutton(2,"Play")
    end if

    if archiveState = 0 then
        springboardScreen.addbutton(4,"Archive recording")
    end if

    springboardScreen.addbutton(3,"Delete recording")

End Function


Function ShowSpringboard(springboardURL as String)

    springboardContent = FetchSpringboardContent(springboardURL)

    port = CreateObject("roMessagePort")
    springboardScreen = CreateObject("roSpringboardScreen")
    springboardScreen.SetTitle("CarbonDVR")
    springboardScreen.SetBreadcrumbText("", springboardContent.TRINTV_ShowName)
    springboardScreen.SetMessagePort(port)
    springboardScreen.SetStaticRatingEnabled(false)
'    springboardScreen.SetDescriptionStyle("video")
    springboardScreen.SetContent(springboardContent)

    EnableSpringboardButtons(springboardScreen, springboardContent)

    springboardScreen.Show() 
	
'    print springboardContent
 
    While True
        EnableSpringboardButtons(springboardScreen, springboardContent)
        msg = wait(0, port)
        If msg.isScreenClosed() Then
            return -1
        else if msg.isButtonPressed() 
            print "ButtonPressed: ";msg.GetIndex()
            if msg.GetIndex() = 1
                playbackPosition = GetPlaybackPosition(springboardContent.TRINTV_GetPositionURL)
                springboardContent.PlayStart = playbackPosition
                showVideoScreen(springboardContent)
'                EnableSpringboardButtons(springboardScreen, springboardContent)
'                refreshShowDetail(screen,showList,showIndex)
            endif
            if msg.GetIndex() = 2
                springboardContent.PlayStart = 0
                showVideoScreen(springboardContent)
'                refreshShowDetail(screen,showList,showIndex)
            endif
            if msg.GetIndex() = 3
                if ConfirmDelete()
                    print "Deleting recording"
                    DeleteRecording(springboardContent.TRINTV_DeleteURL)
	            Return -1
                end if
            endif
            if msg.GetIndex() = 4
                print "Archiving recording"
                ArchiveRecording(springboardContent.TRINTV_ArchiveURL)
		Return -1
            endif
        End If
    End While

End Function


Function ConfirmDelete() as Boolean
    port = CreateObject("roMessagePort")
    dialog = CreateObject("roMessageDialog")
    dialog.SetMessagePort(port)
    dialog.SetTitle("Are you sure?")
    dialog.SetText("Delete this recording?")
    dialog.AddButton(1, "No")
    dialog.AddButton(2, "Yes")
    dialog.Show()

    bDeleteConfirmed = false

    while true
        msg = wait(0, dialog.GetMessagePort())
        if type(msg) = "roMessageDialogEvent"
            if msg.isButtonPressed()
                if msg.GetIndex() = 2
                  bDeleteConfirmed = true
                end if
            exit while
            end if
        end if
    end while

    return bDeleteConfirmed
  
End Function

Function DeleteRecording(deleteURL as String)
    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(deleteURL)
    urlTransfer.SetRequest("DELETE")
    urlTransfer.GetToString()
End Function


Function SetPlaybackPosition(setPositionURL as String, playbackPosition as Integer)
    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(setPositionURL + playbackPosition.toStr())
    urlTransfer.SetRequest("PUT")
    urlTransfer.GetToString()
End Function


Function GetPlaybackPosition(getPositionURL as String) as Integer
    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(getPositionURL)
    response = urlTransfer.GetToString()
    return response.toInt()
End Function


Function ArchiveRecording(archiveURL as String)
    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(archiveURL)
    urlTransfer.SetRequest("PUT")
    urlTransfer.GetToString()
End Function


Function GetArchiveState(getArchiveStateURL as String) as Integer
    urlTransfer = CreateObject("roUrlTransfer")
    urlTransfer.SetURL(getArchiveStateURL)
    response = urlTransfer.GetToString()
    return response.toInt()
End Function


Function showVideoScreen(episode As Object)

    if type(episode) <> "roAssociativeArray" then
        print "invalid data passed to showVideoScreen"
        return -1
    endif

    port = CreateObject("roMessagePort")
    screen = CreateObject("roVideoScreen")
    screen.SetMessagePort(port)

    screen.SetPositionNotificationPeriod(30)
    screen.SetContent(episode)
    screen.Show()

    'Uncomment his line to dump the contents of the episode to be played
    'PrintAA(episode)

    while true
        msg = wait(0, port)

        if type(msg) = "roVideoScreenEvent" then
            if msg.isScreenClosed()
                print "roVideoScreenEvent: Screen Closed"
                exit while
            elseif msg.isRequestFailed()
                print "roVideoScreenEvent: Request Failure: "; msg.GetIndex(); " " msg.GetData() 
            elseif msg.isStatusMessage()
                print "roVideoScreenEvent: Status Message: "; msg.GetIndex(); " " msg.GetData() 
            elseif msg.isButtonPressed()
                print "roVideoScreenEvent: Button Pressed: "; msg.GetIndex(); " " msg.GetData()
            elseif msg.isPlaybackPosition() then
                print "roVideoScreenEvent: Playback Position: Index="; msg.GetIndex()
                nowpos = msg.GetIndex()
                SetPlaybackPosition(episode.TRINTV_SetPositionURL, msg.GetIndex())
            elseif msg.isFullResult() then
                print "roVideoScreenEvent: Full Result"
                SetPlaybackPosition(episode.TRINTV_SetPositionURL, 0)
            elseif msg.IsPartialResult() then
                print "roVideoScreenEvent: Partial Result"
            elseif msg.IsPaused() then
                print "roVideoScreenEvent: Paused"
            elseif msg.IsStreamStarted() then
                print "roVideoScreenEvent: Stream Started: Index="; msg.GetIndex()
            else
                print "Unexpected roVideoScreen event: "; msg.GetType()
            end if
        else
            print "Unexpected message class: "; type(msg)
        end if
    end while

End Function
