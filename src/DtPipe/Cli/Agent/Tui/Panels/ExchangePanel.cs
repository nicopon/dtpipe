using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Drawing;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

namespace DtPipe.Cli.Agent.Tui.Panels;

/// <summary>
/// The exchange between the agent and the user, along the bottom: a marker and a headline that name
/// what just happened and what to do about it, the agent's words below them, and the input line that
/// answers, all inside one frame. The frame is the point — a question and the line that answers it
/// read as one gesture rather than as two unrelated rows of chrome.
///
/// <para>
/// While a turn runs the body is the model's streaming tail, so the surface still shows that the
/// model is producing rather than stalled. The permanent record is not here: <see cref="TranscriptLog"/>
/// keeps every entry and replays it to scrollback at teardown, and the steps list with its detail is
/// where a finished session is read back.
/// </para>
///
/// <para>
/// The body is a list, not a label, because a long question or a long verdict outruns four rows. It
/// does not chase a tail: each word replaces the last one whole, so a new message is shown from its
/// beginning. What grows a line at a time is the running step, and that lives in the detail panel.
/// </para>
/// </summary>
internal sealed class ExchangePanel
{
    private readonly FrameView _frame;
    private readonly Label _headline;
    private readonly ListView _body;
    private string _rendered = string.Empty;
    private IReadOnlyList<string> _lines = Array.Empty<string>();

    public ExchangePanel()
    {
        _frame = new FrameView
        {
            Title = "Agent",
            X = 0,
            Y = Pos.AnchorEnd(TuiScreen.BottomChrome),
            Width = Dim.Fill(),
            Height = Dim.Absolute(TuiScreen.ExchangeHeight),
            CanFocus = true,
        };

        _headline = new Label { X = 1, Y = 0, Width = Dim.Fill(1), Text = string.Empty };
        _body = new ListView
        {
            X = 4,
            Y = 1,
            Width = Dim.Fill(1),
            Height = Dim.Fill(1),          // the last row belongs to the input line
            CanFocus = true,
        };
        _body.SetSource(new ObservableCollection<string>());

        _frame.Add(_headline, _body);
    }

    public View Frame => _frame;
    public View FocusTarget => _body;

    internal string HeadlineText => _headline.Text;
    internal string BodyText => string.Join('\n', Lines());
    internal int? SelectedIndex => _body.SelectedItem;

    private IReadOnlyList<string> Lines() => _lines;

    /// <summary>Re-labels the frame — the running turn's progress, or nothing but the name.</summary>
    public void Retitle(string title)
    {
        if (_frame.Title != title) _frame.Title = title;
    }

    /// <summary>
    /// Shows <paramref name="exchange"/>. A no-op when it renders the same lines, so the repaint
    /// timer does not rebuild the list ten times a second over an unchanged question.
    /// </summary>
    public void Show(Exchange exchange)
    {
        var headline = $"{exchange.Marker}  {exchange.Headline}";
        if (_headline.Text != headline) _headline.Text = headline;

        var lines = exchange.BodyLines();
        var text = string.Join('\n', lines);
        if (text == _rendered) return;
        _rendered = text;
        _lines = lines;

        _body.SetSource(new ObservableCollection<string>(lines));
        if (lines.Count > 0) _body.SelectedItem = 0;   // a message is read from its beginning
    }
}
