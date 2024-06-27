$(document).ready(function() {
    var flyout = document.querySelectorAll('.floating, .container, .bottom-right');
    for (let i = 0; i < flyout.length; i++) {
        flyout.removeClass('floating bottom-right');
        flyout.addClass('wy-nav-side');
    }
});
